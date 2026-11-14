import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd
from queries.github_queries import get_pr_metadata

st.set_page_config(page_title="pipeline-hq · PR Metadata", layout="wide")
st.title("PR Metadata")

@st.cache_data(ttl=60)
def get_pr_data() -> pd.DataFrame:
    return get_pr_metadata()

df = get_pr_data()

col1, col2, col3 = st.columns(3)
col1.metric("Total PRs",   len(df))
col2.metric("Merged",      df["merged"].sum())
col3.metric("Merge rate",  f"{df['merged'].mean():.0%}")

st.dataframe(df, use_container_width=True)