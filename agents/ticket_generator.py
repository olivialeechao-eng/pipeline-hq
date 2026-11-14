"""
jira_agent.py — pipeline-hq Jira Agent

Contract: receives a raw description of a pipeline alert, issue, or feature request
→ generates all structured ticket fields in one LLM call → validates against JiraTicket schema
→ retries with error feedback on failure → saves to tickets/ directory on success.

Graph:
    generate_ticket_fields → validate_ticket → route_after_validation
            ↑                                       |             |
     add_errors_to_state  ←── invalid ──────────────┘             |
                                                              valid |
                                                          save_ticket → END
"""

import json
import os
import textwrap
from typing import TypedDict
from dotenv import load_dotenv
from langgraph.graph import START, END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from openai import OpenAI
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry import trace
from opentelemetry.instrumentation.openai import OpenAIInstrumentor
from openinference.semconv.resource import ResourceAttributes
from opentelemetry.sdk.resources import Resource

from agent_helpers.ticket_schema import (
    AssigneeID, IssueType, JiraTicket, Priority, PipelineID,
    ProjectID, ReporterID, _next_ticket_id,
)


# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

GROQ_API_KEY    = os.environ["GROQ_API_KEY"]
MODEL           = "openai/gpt-oss-120b"
PHOENIX_API_KEY = os.environ["PHOENIX_API_KEY"]
PHOENIX_COLLECTOR_ENDPOINT = os.environ["PHOENIX_COLLECTOR_ENDPOINT"]
TICKETS_DIR     = os.path.join(os.path.dirname(__file__), "..", "tickets")
MAX_RETRIES     = 3

resource = Resource(attributes={
    ResourceAttributes.PROJECT_NAME: "pipeline-hq"
})

exporter = OTLPSpanExporter(
    endpoint=PHOENIX_COLLECTOR_ENDPOINT,
    headers={"Authorization": f"Bearer {PHOENIX_API_KEY}"},
)

provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

OpenAIInstrumentor().instrument()

client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1",
)


# ── State ─────────────────────────────────────────────────────────────────────

class TicketGeneratorState(TypedDict):
    input_description: str    # raw alert/issue text from the caller
    ticket_fields:     dict   # LLM-produced full ticket field values (all fields)
    validation_error:  str    # set by validate_ticket on failure; cleared on success
    retry_count:       int    # incremented by add_errors_to_state; caps at MAX_RETRIES
    ticket:            dict   # final JiraTicket.to_dict() output
    ticket_path:       str    # absolute path where the ticket JSON was saved
    error:             str    # set if the agent gives up after MAX_RETRIES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _call_llm(instructions: str, input: str, max_output_tokens: int) -> str:
    response = client.responses.create(
        model=MODEL,
        instructions=instructions,
        input=input,
        temperature=0.2,
        max_output_tokens=max_output_tokens,
    )
    return response.output_text.strip()

def _strip_fences(raw: str) -> str:
    """Strip markdown code fences if the model added them anyway."""
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


# ── Node 1: generate_ticket_fields ───────────────────────────────────────────

def generate_ticket_fields(state: TicketGeneratorState) -> TicketGeneratorState:
    """
    Generates all ticket fields from the raw description in a single LLM call.
    On retry passes, includes the validation error as feedback so the LLM can fix it.
    """
    attempt = state["retry_count"] + 1
    print(f"[jira_agent] generate_ticket_fields (attempt {attempt})")

    instructions = textwrap.dedent("""
        You are a pipeline operations analyst creating a Jira ticket.

        You will be given a raw description of a pipeline alert, issue, or feature request.
        Generate all ticket fields.

        Output a JSON object with exactly these keys:
        {
          "issuetype": "Bug" | "Task" | "Spike",
          "priority": "Critical" | "High" | "Medium" | "Low",
          "customfield_pipeline_source": "credit_card_fraud_pipeline",
          "summary": "concise title, max 50 characters",
          "description": "detailed explanation including context, symptoms, and known impact",
          "customfield_acceptance_criteria": "clear, testable conditions for the issue to be resolved",
          "customfield_relevant_files": ["relative/path/to/file.py"]
        }

        Guidelines:
        - issuetype: unexpected errors or failures → Bug; investigation/research → Spike; planned work → Task
        - priority: production outage or data loss → Critical; significant impact → High; moderate impact → Medium; cosmetic/minor → Low
        - customfield_pipeline_source: always "credit_card_fraud_pipeline"
        - summary: max 50 characters, must be concise
        - customfield_relevant_files: infer from context (e.g. pipeline name → pipelines/<name>.py); use [] if unknown

        Output only the JSON object. No preamble, no markdown fences.
    """).strip()

    error_feedback = ""
    if state["validation_error"]:
        error_feedback = f"\n\nYour previous attempt failed schema validation with this error:\n  {state['validation_error']}\nFix the issue and try again."

    input = f"{state['input_description']}{error_feedback}"

    raw = _call_llm(instructions=instructions, input=input, max_output_tokens=800)
    try:
        ticket_fields = json.loads(_strip_fences(raw))
    except json.JSONDecodeError as e:
        print(f"[jira_agent] generate_ticket_fields — JSON parse error: {e}")
        return {**state, "ticket_fields": {}, "validation_error": f"Output was not valid JSON: {e}"}

    print(f"[jira_agent] generated fields — summary: {ticket_fields['summary']}")
    return {**state, "ticket_fields": ticket_fields}


# ── Node 2: validate_ticket ───────────────────────────────────────────────────

def validate_ticket(state: TicketGeneratorState) -> TicketGeneratorState:
    """
    Attempts to instantiate JiraTicket from current ticket_fields. If __post_init__
    or enum coercion raises, the error is stored in validation_error for the retry loop.
    Nothing is written to disk here.
    """
    print("[jira_agent] validate_ticket")

    fields = state["ticket_fields"]
    if not fields:
        return state  # preserve validation_error already set (e.g. JSON parse failure)

    try:
        JiraTicket(
            id                              = "TICKET-000",  # dummy id, validation only
            project_id                      = ProjectID.PROJECT_1,
            summary                         = fields["summary"],
            assignee_id                     = AssigneeID.TASK_PR_AGENT,
            issuetype                       = IssueType(fields["issuetype"]),
            priority                        = Priority(fields["priority"]),
            reporter_id                     = ReporterID.TEAM_MEMBER_1,
            description                     = fields["description"],
            customfield_acceptance_criteria = fields["customfield_acceptance_criteria"],
            customfield_pipeline_source     = PipelineID(fields["customfield_pipeline_source"]),
            customfield_relevant_files      = fields["customfield_relevant_files"],
        )
        print("[jira_agent] validate_ticket — OK")
        return {**state, "validation_error": ""}
    except (ValueError, KeyError) as e:
        print(f"[jira_agent] validate_ticket — FAILED: {e}")
        return {**state, "validation_error": str(e)}


# ── Node 3: add_errors_to_state ───────────────────────────────────────────────

def add_errors_to_state(state: TicketGeneratorState) -> TicketGeneratorState:
    """
    Increments the retry counter before looping back to generate_ticket_fields.
    The validation_error already in state is what generate_ticket_fields reads
    to produce corrective feedback for the LLM.
    """
    retry_count = state["retry_count"] + 1
    print(f"[jira_agent] add_errors_to_state — retry {retry_count}/{MAX_RETRIES}: {state['validation_error']}")
    return {**state, "retry_count": retry_count}


# ── Node 4: save_ticket ───────────────────────────────────────────────────────

def save_ticket(state: TicketGeneratorState) -> TicketGeneratorState:
    """
    Instantiates JiraTicket (guaranteed valid at this point) and saves to tickets/.
    """
    print("[jira_agent] save_ticket")

    fields = state["ticket_fields"]

    os.makedirs(TICKETS_DIR, exist_ok=True)
    ticket_id = _next_ticket_id(TICKETS_DIR)

    jira_ticket = JiraTicket(
        id                              = ticket_id,
        project_id                      = ProjectID.PROJECT_1,
        summary                         = fields["summary"],
        assignee_id                     = AssigneeID.TASK_PR_AGENT,
        issuetype                       = IssueType(fields["issuetype"]),
        priority                        = Priority(fields["priority"]),
        reporter_id                     = ReporterID.TEAM_MEMBER_1,
        description                     = fields["description"],
        customfield_acceptance_criteria = fields["customfield_acceptance_criteria"],
        customfield_pipeline_source     = PipelineID(fields["customfield_pipeline_source"]),
        customfield_relevant_files      = fields["customfield_relevant_files"],
    )

    ticket_dict = jira_ticket.to_dict()
    ticket_path = os.path.join(TICKETS_DIR, f"{ticket_id}.json")

    with open(ticket_path, "w") as f:
        json.dump(ticket_dict, f, indent=2)

    print(f"[jira_agent] saved {ticket_path}")
    return {**state, "ticket": ticket_dict, "ticket_path": ticket_path}


# ── Conditional edge ──────────────────────────────────────────────────────────

def route_after_validation(state: TicketGeneratorState) -> str:
    """
    Routes to save_ticket on success.
    Routes to add_errors_to_state for retry up to MAX_RETRIES.
    Routes to END (with error set) if retries are exhausted.
    """
    if not state["validation_error"]:
        return "save_ticket"
    if state["retry_count"] >= MAX_RETRIES:
        print(f"[jira_agent] route_after_validation — giving up after {MAX_RETRIES} retries")
        return END
    return "add_errors_to_state"


# ── Graph ─────────────────────────────────────────────────────────────────────

def build_jira_agent() -> CompiledStateGraph:
    graph = StateGraph(TicketGeneratorState)

    graph.add_node("generate_ticket_fields", generate_ticket_fields)
    graph.add_node("validate_ticket",        validate_ticket)
    graph.add_node("add_errors_to_state",    add_errors_to_state)
    graph.add_node("save_ticket",            save_ticket)

    graph.add_edge(START,                    "generate_ticket_fields")
    graph.add_edge("generate_ticket_fields", "validate_ticket")
    graph.add_edge("add_errors_to_state",    "generate_ticket_fields")
    graph.add_edge("save_ticket",            END)

    graph.add_conditional_edges(
        "validate_ticket",
        route_after_validation,
        {
            "save_ticket":         "save_ticket",
            "add_errors_to_state": "add_errors_to_state",
            END:                   END,
        },
    )

    return graph.compile()


jira_agent = build_jira_agent()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_agent(description: str) -> str:
    """
    Called by the Router or pipeline monitoring system with a raw alert/issue description.
    Returns the path of the saved ticket JSON.

    Example:
        ticket_path = run_agent("The fraud pipeline failed with a KeyError on merchant_category.")
    """
    initial_state: TicketGeneratorState = {
        "input_description": description,
        "ticket_fields":     {},
        "validation_error":  "",
        "retry_count":       0,
        "ticket":            {},
        "ticket_path":       "",
        "error":             "",
    }

    final_state = jira_agent.invoke(initial_state)

    if final_state.get("error"):
        raise RuntimeError(f"jira_agent failed: {final_state['error']}")
    if not final_state.get("ticket_path"):
        raise RuntimeError(f"jira_agent gave up after {MAX_RETRIES} retries: {final_state.get('validation_error')}")

    return final_state["ticket_path"]


# ── Local test ────────────────────────────────────────────────────────────────
#
# Runs all nodes in isolation, then exercises the retry loop, then runs the full graph.
# No external dependencies required — safe to run locally.
#
# Usage:
#   GROQ_API_KEY=your_key PHOENIX_API_KEY=your_key python -m agents.jira_agent

TEST_DESCRIPTION = (
    "change credit card fraud pipeline to use 20 partitions instead of 10"
)

if __name__ == "__main__":
    base_state: TicketGeneratorState = {
        "input_description": TEST_DESCRIPTION,
        "ticket_fields":     {},
        "validation_error":  "",
        "retry_count":       0,
        "ticket":            {},
        "ticket_path":       "",
        "error":             "",
    }

    # ── Node isolation tests ──────────────────────────────────────────────────

    print("=" * 60)
    print("NODE 1: generate_ticket_fields (first attempt)")
    print("=" * 60)
    s1 = generate_ticket_fields(base_state)
    assert s1["ticket_fields"], "ticket_fields should be populated"
    expected_keys = {
        "issuetype", "priority", "customfield_pipeline_source",
        "summary", "description", "customfield_acceptance_criteria",
        "customfield_relevant_files",
    }
    missing = expected_keys - s1["ticket_fields"].keys()
    assert not missing, f"ticket_fields missing keys: {missing}"
    assert len(s1["ticket_fields"]["summary"]) <= 50, "summary exceeds 50 chars"
    print("OK — ticket_fields JSON valid, all keys present")
    print("issuetype:                  ", s1["ticket_fields"]["issuetype"])
    print("priority:                   ", s1["ticket_fields"]["priority"])
    print("customfield_pipeline_source:", s1["ticket_fields"]["customfield_pipeline_source"])
    print("summary:                    ", s1["ticket_fields"]["summary"])
    print("relevant_files:             ", s1["ticket_fields"]["customfield_relevant_files"])

    print()
    print("=" * 60)
    print("NODE 2: validate_ticket (valid fields)")
    print("=" * 60)
    s2_ok = validate_ticket(s1)
    assert s2_ok["validation_error"] == "", f"expected no error, got: {s2_ok['validation_error']}"
    print("OK — validation passed")

    print()
    print("=" * 60)
    print("NODE 2: validate_ticket (bad summary — retry path)")
    print("=" * 60)
    bad_state: TicketGeneratorState = {**s1, "ticket_fields": {**s1["ticket_fields"], "summary": "x" * 51}}
    s2_bad = validate_ticket(bad_state)
    assert s2_bad["validation_error"], "expected a validation error"
    print(f"OK — caught error: {s2_bad['validation_error']}")

    print()
    print("=" * 60)
    print("NODE 3: add_errors_to_state")
    print("=" * 60)
    s3 = add_errors_to_state(s2_bad)
    assert s3["retry_count"] == 1, f"expected retry_count=1, got {s3['retry_count']}"
    print(f"OK — retry_count incremented to {s3['retry_count']}")

    print()
    print("=" * 60)
    print("NODE 1: generate_ticket_fields (retry with error feedback)")
    print("=" * 60)
    s3b = generate_ticket_fields(s3)
    assert len(s3b["ticket_fields"]["summary"]) <= 50, "summary still exceeds 50 chars after retry"
    print(f"OK — corrected summary: {s3b['ticket_fields']['summary']}")

    print()
    print("=" * 60)
    print("NODE 4: save_ticket")
    print("=" * 60)
    s4 = save_ticket(s2_ok)
    assert s4["ticket"], "ticket dict should be populated"
    assert s4["ticket_path"], "ticket_path should be set"
    assert os.path.exists(s4["ticket_path"]), "ticket file should exist on disk"
    print(f"OK — ticket saved: {s4['ticket_path']}")
    print(f"OK — ticket id:    {s4['ticket']['id']}")

    # ── Full graph run ────────────────────────────────────────────────────────

    print()
    print("=" * 60)
    print("FULL GRAPH RUN")
    print("=" * 60)
    ticket_path = run_agent(TEST_DESCRIPTION)
    print(f"OK — ticket_path: {ticket_path}")

    print()
    print("=" * 60)
    print("ALL CHECKS PASSED")
    print("=" * 60)
