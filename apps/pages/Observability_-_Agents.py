import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd
from queries.phoenix_queries import get_agent_spans
from ingestion.phoenix_ingest_traces import ingest

st.set_page_config(page_title="pipeline-hq · Agents", layout="wide")
st.title("Agents")

start_date = st.date_input("Select start date", value=pd.Timestamp.now().date() - pd.Timedelta(days=1))


@st.cache_data(ttl=60)
def get_spans_df(start_date: str) -> pd.DataFrame:
    return get_agent_spans(start_date)


if st.button("Ingest latest spans"):
    with st.spinner("Fetching from Phoenix and writing to Iceberg..."):
        n = ingest(start_date=pd.Timestamp(start_date))
    st.success(f"Ingested {n} spans")
    get_spans_df.clear()
    st.rerun()

spans_df = get_spans_df(str(start_date)) # each row is 1 span

if spans_df.empty:
    st.info("No data yet — click 'Ingest latest spans' to load from Phoenix.")
    st.stop()

root_spans_df = spans_df[spans_df["parent_id"].isna()] # each row is 1 root span (parent)

ALL_COLS = [
    "trace_id", "span_id", "name", "span_kind",
    "start_time", "end_time", "latency_ms",
    "status_code", "status_message", "parent_id",
    "llm_model_name", "llm_provider", "llm_system", "llm_finish_reason",
    "token_count_total", "token_count_prompt", "token_count_cache_read", "token_count_completion", "cost_usd",
    "output_value", "output_mime_type", "openinference_span_kind",
    "llm_invocation_parameters", "llm_input_messages", "llm_output_messages",
    "events", "gen_ai", "openai",
]

root_span_cols = [c for c in ALL_COLS if c in root_spans_df.columns]

COST_COL_CONFIG = {
    "cost_usd": st.column_config.NumberColumn("cost_usd", format="$%.6f"),
}

st.subheader("Root Spans")
st.dataframe(root_spans_df[root_span_cols], column_config=COST_COL_CONFIG)

st.subheader("Child Spans")
root_span_ids_with_children = spans_df[spans_df["parent_id"].notna()]["trace_id"].unique()
selected = st.selectbox("Select a trace_id to inspect", root_spans_df[root_spans_df["trace_id"].isin(root_span_ids_with_children)]["trace_id"])

if selected:
    child_spans = spans_df[(spans_df["trace_id"] == selected) & spans_df["parent_id"].notna()]
    span_cols = [c for c in ALL_COLS if c in child_spans.columns]
    st.subheader(f"Child spans for {selected}")
    st.dataframe(child_spans[span_cols], column_config=COST_COL_CONFIG)
