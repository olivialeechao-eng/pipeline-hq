import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
from agents.ticket_generator import run_agent

st.set_page_config(page_title="Submit Ticket", layout="centered")
st.title("Submit a ticket")
st.caption("Describe the issue or change in plain English. The ticket generator formats it.")

description = st.text_area("Description", height=120, placeholder="e.g. The fraud pipeline is partitioning into 10 files — change it to 20")

if st.button("Generate & queue", type="primary", disabled=not description.strip()):
    with st.spinner("Ticket generator running..."):
        try:
            ticket_path = run_agent(description)
            st.success(f"Ticket queued → {Path(ticket_path).name}")
            st.caption("Go to the main page to review and approve it.")
        except Exception as e:
            st.error(f"Agent failed: {e}")