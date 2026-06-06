"""
app.py — pipeline-hq demo

To run: 
streamlit run apps/Ticket_Queue.py

Reads tickets from tickets/ → human approves → Task Agent runs → PR link
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
from agents.task_pr_agent import run_agent

TICKETS_DIR = "tickets"

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_pending_tickets() -> list[dict]:
    """Load all tickets that haven't been approved yet."""
    tickets = []
    for path in sorted(os.listdir(TICKETS_DIR)):
        if not path.endswith(".json"):
            continue
        with open(os.path.join(TICKETS_DIR, path)) as f:
            ticket = json.load(f)
        # Only show tickets that are still New
        if ticket["fields"].get("customfield_status") == "New":
            tickets.append(ticket)
    return tickets

def get_text(doc_field: dict) -> str:
    return " ".join(
        node["text"]
        for block in doc_field["content"]
        for node in block.get("content", [])
        if node.get("type") == "text"
    )

def mark_ticket_status(ticket: dict, status: str) -> None:
    """Write status change back to the JSON file on disk."""
    ticket_id = ticket["id"]
    path = os.path.join(TICKETS_DIR, f"{ticket_id}.json")
    # find the actual file — name might include a slug
    for filename in os.listdir(TICKETS_DIR):
        if filename.startswith(ticket_id) and filename.endswith(".json"):
            path = os.path.join(TICKETS_DIR, filename)
            break
    ticket["fields"]["customfield_status"] = status
    with open(path, "w") as f:
        json.dump(ticket, f, indent=2)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="pipeline-hq", layout="centered")
st.title("Ticket Queue")
st.caption("Review pending tickets. Approve to dispatch to an agent.")

# ── No tickets ────────────────────────────────────────────────────────────────

if not os.path.exists(TICKETS_DIR):
    st.info("No tickets directory found. Submit a ticket first.")
    st.stop()

tickets = load_pending_tickets()

if not tickets and "pr_url" not in st.session_state and "approved_ticket" not in st.session_state:
    st.info("No pending tickets. Submit a ticket first.")
    if st.button("↻ Refresh"):
        st.rerun()
    st.stop()

# ── Step 1: ticket approval ───────────────────────────────────────────────────

if "approved_ticket" not in st.session_state and "pr_url" not in st.session_state:

    st.subheader(f"{len(tickets)} pending ticket{'s' if len(tickets) > 1 else ''}")

    for ticket in tickets:
        fields = ticket["fields"]

        with st.container(border=True):
            col_id, col_type, col_priority = st.columns(3)
            col_id.markdown(f"`{ticket['id']}`")
            col_type.markdown(f"**{fields['issuetype']['name']}**")
            col_priority.markdown(fields["priority"]["name"])

            st.markdown(f"#### {fields['summary']}")
            st.markdown(get_text(fields["description"]))

            with st.expander("Acceptance criteria"):
                st.markdown(get_text(fields["customfield_acceptance_criteria"]))

            with st.expander("Relevant files"):
                for f in fields["customfield_relevant_files"]:
                    st.code(f)

            col1, col2 = st.columns(2)

            if col1.button("✓ Approve", key=f"approve_{ticket['id']}", type="primary", use_container_width=True):
                mark_ticket_status(ticket, "Ready for Dev")
                st.session_state.approved_ticket = ticket
                st.rerun()

            if col2.button("✕ Reject", key=f"reject_{ticket['id']}", use_container_width=True):
                mark_ticket_status(ticket, "Backlog")
                st.rerun()

# ── Step 2: agent runs ────────────────────────────────────────────────────────

elif "approved_ticket" in st.session_state and "pr_url" not in st.session_state:

    ticket = st.session_state.approved_ticket
    fields = ticket["fields"]

    st.subheader("Agent running")
    st.markdown(f"**{fields['summary']}**")
    st.caption(f"{fields['issuetype']['name']} · {fields['priority']['name']}")
    st.divider()

    with st.spinner("Task Agent working..."):
        try:
            pr_url = run_agent(ticket)
            mark_ticket_status(ticket, "PR Opened")
            st.session_state.pr_url = pr_url
            st.rerun()
        except Exception as e:
            st.error(f"Agent failed: {e}")
            if st.button("Start over"):
                st.session_state.clear()
                st.rerun()

# ── Step 3: done ──────────────────────────────────────────────────────────────

elif "pr_url" in st.session_state:

    ticket = st.session_state.approved_ticket
    fields = ticket["fields"]

    st.success("PR opened")
    st.markdown(f"**{fields['summary']}**")
    st.markdown(f"[View PR →]({st.session_state.pr_url})")
    st.caption("Review it in GitHub. Merge, request changes, or close it.")

    if st.button("↻ Check for more tickets"):
        st.session_state.clear()
        st.rerun()