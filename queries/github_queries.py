import duckdb
import pandas as pd
from pyiceberg.exceptions import NoSuchTableError

from ingestion.github_ingest_pr_metadata import load_pr_metadata_table

_iceberg_loaded = False


def _ensure_iceberg() -> None:
    global _iceberg_loaded
    if not _iceberg_loaded:
        duckdb.sql("INSTALL iceberg; LOAD iceberg;")
        _iceberg_loaded = True


def get_pr_metadata() -> pd.DataFrame:
    try:
        table = load_pr_metadata_table()
    except NoSuchTableError:
        return pd.DataFrame()

    _ensure_iceberg()
    return duckdb.sql(f"""
        SELECT
            pr_number,
            title,
            author,
            state,
            merged,
            created_at,
            merged_at,
            commits,
            additions,
            deletions,
            changed_files,
            review_comments,
            comments
        FROM iceberg_scan('{table.metadata_location}')
        ORDER BY created_at DESC
    """).df()
