"""
task_agent.py — pipeline-hq Task Agent

Contract: receives a Task ticket from the Router → implements the change → opens a GitHub PR
→ waits for human review → loops back on CHANGES_REQUESTED, ends on merge.

Graph:
    load_relevant_files → plan_changes → implement_changes → open_pr → get_human_approval
                ↑                                                             |
                └─────────────── CHANGES_REQUESTED ──────────────────────────┘
                                                                              |
                                                                             END (merged or closed)

Prefect's only job is to call task_agent.invoke() on a schedule.
All routing logic lives here — switching orchestrators requires no changes to this file.
"""

import json
import os
import textwrap
from datetime import datetime, timezone
from typing import TypedDict
from dotenv import load_dotenv
from github import Github
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig
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


# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "")
MODEL        = "openai/gpt-oss-120b"
PHOENIX_API_KEY = os.environ["PHOENIX_API_KEY"]

resource = Resource(attributes={
    ResourceAttributes.PROJECT_NAME: "pipeline-hq"
})

exporter = OTLPSpanExporter(
    endpoint="https://app.phoenix.arize.com/s/olivialeechao/v1/traces",
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

gh = Github(GITHUB_TOKEN) if GITHUB_TOKEN else None


# ── State ─────────────────────────────────────────────────────────────────────

class TaskAgentState(TypedDict):
    ticket:            dict              # full Jira-schema ticket from the Router
    files:             dict[str, str]    # {file_path: content}, populated by load_relevant_files
    plan:              str               # LLM-produced change plan (JSON string)
    diffs:             list[dict]        # [{file_path, new_content}]
    pr_url:            str               # URL of the opened PR
    pr_number:         int               # GitHub PR number, needed for review polling
    pr_comments:       list[dict]        # [{author, body, path, line}] — review comments from the PR
    last_agent_run_at: str               # ISO timestamp — updated each time the agent runs
    error:             str               # set if any node fails, surfaces to caller


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_ticket_id(state: TaskAgentState) -> str:
    if "id" not in state["ticket"]:
        raise KeyError(f"Ticket missing required 'id' field: {state['ticket']}")
    return state["ticket"]["id"]

def _get_ticket_summary(state: TaskAgentState) -> str:
    return state["ticket"]["fields"]["summary"]

def _get_ticket_description(state: TaskAgentState) -> str:
    """Extract plain text from Jira doc-format description."""
    content = state["ticket"]["fields"]["description"]["content"]
    return " ".join(
        node["text"]
        for block in content
        for node in block.get("content", [])
        if node.get("type") == "text"
    )

def _get_acceptance_criteria(state: TaskAgentState) -> str:
    content = state["ticket"]["fields"]["customfield_acceptance_criteria"]["content"]
    return " ".join(
        node["text"]
        for block in content
        for node in block.get("content", [])
        if node.get("type") == "text"
    )

def _get_relevant_files(state: TaskAgentState) -> list[str]:
    return state["ticket"]["fields"]["customfield_relevant_files"]

def _get_branch_name(state: TaskAgentState) -> str:
    slug = _get_ticket_summary(state).lower()
    slug = "".join(c if c.isalnum() else "-" for c in slug)[:40].strip("-")
    return f"task/{_get_ticket_id(state).lower()}-{slug}"

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

def _get_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Node 1: load_relevant_files ───────────────────────────────────────────────

def load_relevant_files(state: TaskAgentState) -> TaskAgentState:
    """
    Fetches the current content of every file listed in the ticket's
    customfield_relevant_files. On rework passes this also picks up any
    changes the human pushed to the branch since the last agent run.

    Local fallback is used when GITHUB_TOKEN is not set (dev / test mode).
    """
    print(f"[task_agent] load_relevant_files — ticket: {_get_ticket_id(state)}")

    files: dict[str, str] = {}

    for path in _get_relevant_files(state):
        if gh and GITHUB_REPO:
            repo = gh.get_repo(GITHUB_REPO)
            f    = repo.get_contents(path, ref="main")
            assert not isinstance(f, list)
            files[path] = f.decoded_content.decode("utf-8")
        else:
            repo_root  = os.path.join(os.path.dirname(__file__), "..")
            local_path = os.path.join(repo_root, path)
            with open(local_path) as f:
                files[path] = f.read()

        print(f"[task_agent] loaded {path} ({len(files[path])} chars)")

    return {**state, "files": files}


# ── Node 2: plan_changes ──────────────────────────────────────────────────────

def plan_changes(state: TaskAgentState) -> TaskAgentState:
    """
    Produces a structured change plan from the ticket and current file content.
    On rework passes, pr_comments contains the reviewer's inline and top-level
    feedback fetched by route_after_approval — included in the LLM input directly.
    """
    print(f"[task_agent] plan_changes — ticket: {_get_ticket_id(state)}")

    instructions = textwrap.dedent("""
        You are a data engineering agent. Your job is to plan discrete changes
        based on a Jira ticket.

        You will be given:
        - A task ticket describing what needs to change
        - The current pipeline code
        - (On rework passes) PR review comments from the human reviewer

        If PR review comments are present, treat them as ground truth and
        incorporate them into your plan ahead of the original ticket description.

        Output a JSON object with exactly these keys:
        {
          "files_to_change": ["filename.py"],
          "change_description": "one sentence describing the change",
          "reasoning": "why this change addresses the ticket",
          "approach": "step-by-step plan of what to modify in the code"
        }

        Output only the JSON object. No preamble, no explanation, no markdown fences.
    """).strip()

    files_section = "\n\n".join(
        f"{path}:\n```python\n{content}\n```"
        for path, content in state["files"].items()
    )

    pr_comments = state.get("pr_comments") or []
    comments_section = ""
    if pr_comments:
        lines = []
        for c in pr_comments:
            location = f"{c['path']}:{c['line']}" if c.get("path") else "general"
            lines.append(f"- [{location}] {c['author']}: {c['body']}")
        comments_section = "\n\nPR review comments (address these):\n" + "\n".join(lines)

    input = textwrap.dedent(f"""
        Ticket key: {_get_ticket_id(state)}
        Summary: {_get_ticket_summary(state)}
        Description: {_get_ticket_description(state)}
        Acceptance criteria: {_get_acceptance_criteria(state)}{comments_section}

        Current file content:
        {files_section}
    """).strip()

    raw  = _call_llm(instructions=instructions, input=input, max_output_tokens=1500)
    plan = json.loads(_strip_fences(raw))

    print(f"[task_agent] plan: {plan['change_description']}")
    return {**state, "plan": json.dumps(plan)}


# ── Node 3: implement_changes ─────────────────────────────────────────────────

def implement_changes(state: TaskAgentState) -> TaskAgentState:
    """Produces complete new file content for each file in the plan."""
    print(f"[task_agent] implement_changes — ticket: {_get_ticket_id(state)}")

    plan = json.loads(state["plan"])

    instructions = textwrap.dedent("""
        You are a data engineering agent. Implement a planned change.

        You will be given:
        - The change plan
        - The current content of each file to modify

        Output a JSON array. Each element has exactly these keys:
        {
          "file_path": "relative/path/to/file.py",
          "new_content": "complete new file content as a string"
        }

        Rules:
        - Output the COMPLETE file content for each file, not just the changed lines.
        - Do not truncate or summarise unchanged sections.
        - Preserve all existing comments and structure unless the plan says to change them.
        - Output only the JSON array. No preamble, no markdown fences.
    """).strip()

    files_section = "\n\n".join(
        f"{path}:\n```python\n{content}\n```"
        for path, content in state["files"].items()
    )

    input = textwrap.dedent(f"""
        Change plan:
        {json.dumps(plan, indent=2)}

        File contents:
        {files_section}
    """).strip()

    raw   = _call_llm(instructions=instructions, input=input, max_output_tokens=4096)
    diffs = json.loads(_strip_fences(raw))

    print(f"[task_agent] implement_changes — {len(diffs)} file(s) to write")
    return {**state, "diffs": diffs}


# ── Node 4: open_pr ───────────────────────────────────────────────────────────

def open_pr(state: TaskAgentState) -> TaskAgentState:
    """
    Creates a branch, commits all diffs, and opens a PR.
    On rework passes, pushes new commits to the existing branch rather than
    opening a second PR — the human's review thread stays intact.

    Sets last_agent_run_at so get_human_approval knows which reviews post-date
    this run.
    """
    plan       = json.loads(state["plan"])
    ticket_key = _get_ticket_id(state)
    branch     = _get_branch_name(state)
    commit_msg = f"{ticket_key}: {plan['change_description']}"

    if gh is None:
        raise RuntimeError("GITHUB_TOKEN is not set")
    repo     = gh.get_repo(GITHUB_REPO)
    main_sha = repo.get_branch("main").commit.sha

    # Create branch on first pass; on rework the branch already exists
    existing_branches = [b.name for b in repo.get_branches()]
    if branch not in existing_branches:
        repo.create_git_ref(ref=f"refs/heads/{branch}", sha=main_sha)
        print(f"[task_agent] created branch: {branch}")
    else:
        print(f"[task_agent] branch exists, pushing new commits: {branch}")

    for diff in state["diffs"]:
        file_path   = diff["file_path"]
        new_content = diff["new_content"]
        try:
            existing = repo.get_contents(file_path, ref=branch)
            assert not isinstance(existing, list)
            repo.update_file(
                path=file_path, message=commit_msg,
                content=new_content, sha=existing.sha, branch=branch,
            )
        except Exception:
            repo.create_file(
                path=file_path, message=commit_msg,
                content=new_content, branch=branch,
            )
        print(f"[task_agent] committed {file_path}")

    # Open PR on first pass; on rework the PR already exists
    open_prs = list(repo.get_pulls(state="open", head=branch))
    if open_prs:
        pr = open_prs[0]
        print(f"[task_agent] PR already open, new commits pushed: {pr.html_url}")
    else:
        pr_body = textwrap.dedent(f"""
            **Ticket:** {ticket_key}
            **Summary:** {_get_ticket_summary(state)}

            **What changed:**
            {plan['change_description']}

            **Why:**
            {plan['reasoning']}

            **Approach:**
            {plan['approach']}

            ---
            *Opened by pipeline-hq task-agent · Closes {ticket_key}*
        """).strip()

        pr = repo.create_pull(
            title=f"{ticket_key}: {plan['change_description']}",
            body=pr_body,
            head=branch,
            base="main",
        )
        print(f"[task_agent] PR opened: {pr.html_url}")

    return {
        **state,
        "pr_url":            pr.html_url,
        "pr_number":         pr.number,
        "last_agent_run_at": _get_now_iso(),
    }


# ── Node 5: get_human_approval ────────────────────────────────────────────────────

def get_human_approval(state: TaskAgentState) -> TaskAgentState:
    """
    Polls GitHub for the PR status. This node is a pure pass-through —
    all routing decisions happen in route_after_approval.

    Prefect wakes this node on a schedule by calling task_agent.invoke()
    with the thread_id. No routing logic lives in Prefect.
    """
    print(f"[task_agent] get_human_approval — checking PR #{state['pr_number']}")
    return state


# ── Conditional edge ──────────────────────────────────────────────────────────

def route_after_approval(state: TaskAgentState) -> str:
    """
    Routing logic for the human approval decision:

    - merged            → END
    - closed no merge   → END  (Agent Detector will surface this as a new ticket)
    - CHANGES_REQUESTED since last run → load_relevant_files (rework loop)
    - anything else     → get_human_approval  (no new review yet — stay and wait)

    On CHANGES_REQUESTED, fetches all review comments posted after last_agent_run_at
    and stores them in state["pr_comments"] so plan_changes can incorporate them
    directly — no Router involvement needed.

    last_agent_run_at is set in open_pr so we only react to reviews that
    arrived after the agent's most recent push. This prevents re-triggering
    on reviews from earlier passes.
    """
    if gh is None:
        raise RuntimeError("GITHUB_TOKEN is not set")
    repo = gh.get_repo(GITHUB_REPO)
    pr   = repo.get_pull(state["pr_number"])

    if pr.merged:
        print(f"[task_agent] PR #{pr.number} merged — done")
        return END

    if pr.state == "closed":
        print(f"[task_agent] PR #{pr.number} closed without merge")
        return END

    last_run = datetime.fromisoformat(state["last_agent_run_at"])
    reviews  = pr.get_reviews()

    new_changes_requested = any(
        r.state == "CHANGES_REQUESTED"
        and r.submitted_at.replace(tzinfo=timezone.utc) > last_run
        for r in reviews
    )

    if new_changes_requested:
        # Collect all inline review comments and top-level PR comments posted
        # after the agent's last push — these are the human's concrete feedback.
        pr_comments: list[dict] = []

        for comment in pr.get_review_comments():
            if comment.created_at.replace(tzinfo=timezone.utc) > last_run:
                pr_comments.append({
                    "author": comment.user.login,
                    "body":   comment.body,
                    "path":   comment.path,
                    "line":   comment.original_line,
                })

        for comment in pr.get_issue_comments():
            if comment.created_at.replace(tzinfo=timezone.utc) > last_run:
                pr_comments.append({
                    "author": comment.user.login,
                    "body":   comment.body,
                    "path":   None,
                    "line":   None,
                })

        print(f"[task_agent] new CHANGES_REQUESTED — {len(pr_comments)} comments — rework")
        state["pr_comments"] = pr_comments
        return "load_relevant_files"

    print(f"[task_agent] no new review — waiting")
    return "get_human_approval"


# ── Graph ─────────────────────────────────────────────────────────────────────

def build_task_agent() -> CompiledStateGraph:
    graph = StateGraph(TaskAgentState)

    graph.add_node("load_relevant_files", load_relevant_files)
    graph.add_node("plan_changes",        plan_changes)
    graph.add_node("implement_changes",   implement_changes)
    graph.add_node("open_pr",             open_pr)
    graph.add_node("get_human_approval",      get_human_approval)

    graph.add_edge(START,                 "load_relevant_files")
    graph.add_edge("load_relevant_files", "plan_changes")
    graph.add_edge("plan_changes",        "implement_changes")
    graph.add_edge("implement_changes",   "open_pr")
    graph.add_edge("open_pr",             "get_human_approval")

    graph.add_conditional_edges(
        "get_human_approval",
        route_after_approval,
        {
            "load_relevant_files": "load_relevant_files",
            "get_human_approval":      "get_human_approval",
            END:                   END,
        },
    )

    # MemorySaver for local dev — swap for NeonSaver (Postgres) in production.
    # thread_id is the ticket ID; each ticket gets its own isolated graph state.
    checkpointer = MemorySaver()
    return graph.compile(
        checkpointer=checkpointer,
        interrupt_before=["get_human_approval"],  # pause here, wait for Prefect to resume
    )


task_agent = build_task_agent()


# ── Entry points ───────────────────────────────────────────────────────────────

def run_agent(ticket: dict) -> str:
    """
    Called by the Router on first invocation.
    Returns the PR URL once the graph reaches open_pr.

    The graph then pauses at get_human_approval. Prefect resumes it on a schedule
    by calling poll_pr_status(ticket_id). All routing lives in route_after_approval.

    Example:
        pr_url = run_agent(ticket)
    """
    initial_state: TaskAgentState = {
        "ticket":            ticket,
        "files":             {},
        "plan":              "",
        "diffs":             [],
        "pr_url":            "",
        "pr_number":         0,
        "pr_comments":       [],
        "last_agent_run_at": "",
        "error":             "",
    }

    config: RunnableConfig = {"configurable": {"thread_id": ticket["id"]}}
    final_state = task_agent.invoke(initial_state, config=config)

    if final_state.get("error"):
        raise RuntimeError(f"task_agent failed: {final_state['error']}")

    return final_state["pr_url"]


def poll_pr_status(ticket_id: str) -> None:
    """
    Called by Prefect on a schedule after the PR is open.
    Wakes the graph at get_human_approval — routing logic decides what happens next.

    Prefect knows nothing about GitHub or review state. It just pokes the graph.

    Example (Prefect task):
        @task
        def poll_pr(ticket_id: str) -> None:
            task_agent_module.poll_pr_status(ticket_id)
    """
    config: RunnableConfig = {"configurable": {"thread_id": ticket_id}}
    task_agent.invoke(None, config=config)


# ── Local test ────────────────────────────────────────────────────────────────
#
# Runs all nodes in isolation, then runs the full graph.
# GitHub and Prefect integrations are stubbed — safe to run locally.
#
# Usage:
#   GROQ_API_KEY=your_key python task_agent.py

if __name__ == "__main__":
    with open(os.path.join(os.path.dirname(__file__), "..", "tickets", "TICKET-001.json")) as _f:
        TEST_TICKET = json.load(_f)

    initial_state: TaskAgentState = {
        "ticket":            TEST_TICKET,
        "files":             {},
        "plan":              "",
        "diffs":             [],
        "pr_url":            "",
        "pr_number":         0,
        "pr_comments":       [],
        "last_agent_run_at": "",
        "error":             "",
    }

    # ── Node isolation tests ──────────────────────────────────────────────────

    print("=" * 60)
    print("NODE 1: load_relevant_files")
    print("=" * 60)
    s1 = load_relevant_files(initial_state)
    assert s1["files"], "files should be populated"
    for path, content in s1["files"].items():
        print(f"OK — loaded {path} ({len(content)} chars)")

    print()
    print("=" * 60)
    print("NODE 2: plan_changes")
    print("=" * 60)
    s2 = plan_changes(s1)
    plan = json.loads(s2["plan"])
    missing = {"files_to_change", "change_description", "reasoning", "approach"} - plan.keys()
    assert not missing, f"plan missing keys: {missing}"
    print("OK — plan JSON valid, all keys present")
    print("files_to_change:   ", plan["files_to_change"])
    print("change_description:", plan["change_description"])
    print("reasoning:         ", plan["reasoning"])
    print()
    print("approach:")
    print(plan["approach"])

    print()
    print("=" * 60)
    print("NODE 3: implement_changes")
    print("=" * 60)
    s3 = implement_changes(s2)
    assert s3["diffs"], "diffs should be non-empty"
    for d in s3["diffs"]:
        assert "file_path" in d and "new_content" in d, f"malformed diff: {d}"
    print(f"OK — {len(s3['diffs'])} diff(s) generated")
    for d in s3["diffs"]:
        print(f"\n--- {d['file_path']} ---")
        print(d["new_content"])

    print()
    print("=" * 60)
    print("NODE 4: open_pr")
    print("=" * 60)
    s4 = open_pr(s3)
    assert s4["pr_url"], "pr_url should be set"
    assert s4["last_agent_run_at"], "last_agent_run_at should be set"
    print(f"OK — stub PR URL:        {s4['pr_url']}")
    print(f"OK — last_agent_run_at:  {s4['last_agent_run_at']}")

    print()
    print("=" * 60)
    print("NODE 5: get_human_approval + routing")
    print("=" * 60)
    s5    = get_human_approval(s4)
    route = route_after_approval(s5)
    print(f"OK — route: {route}")

    # ── Full graph run ────────────────────────────────────────────────────────

    print()
    print("=" * 60)
    print("FULL GRAPH RUN")
    print("=" * 60)
    pr_url = run_agent(TEST_TICKET)
    print(f"OK — pr_url: {pr_url}")

    print()
    print("=" * 60)
    print("ALL CHECKS PASSED")
    print("=" * 60)
