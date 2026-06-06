import duckdb
import pandas as pd
from pyiceberg.exceptions import NoSuchTableError

from ingestion.phoenix_ingest_traces import load_agent_spans_table

_iceberg_loaded = False


def _ensure_iceberg() -> None:
    global _iceberg_loaded
    if not _iceberg_loaded:
        duckdb.sql("LOAD iceberg;")
        _iceberg_loaded = True


def get_agent_spans(start_date: str) -> pd.DataFrame:
    try:
        table = load_agent_spans_table()
    except NoSuchTableError:
        return pd.DataFrame()

    _ensure_iceberg()
    # All transformations kept in SQL to ease future migration to dbt.
    df = duckdb.sql(f"""
        SELECT
            trace_id,
            span_id,
            name,
            span_kind,
            start_time,
            end_time,
            date_diff('millisecond', start_time, end_time) AS latency_ms,
            CASE WHEN status_code = 'UNSET' THEN 'SUCCESS' ELSE status_code END AS status_code,
            status_message,
            parent_id,
            events,
            llm_model_name,
            llm_provider,
            llm_system,
            llm_finish_reason,
            llm_invocation_parameters,
            llm_input_messages,
            llm_output_messages,
            token_count_total,
            token_count_prompt,
            token_count_cache_read,
            token_count_completion,
            output_value,
            output_mime_type,
            openinference_span_kind,
            gen_ai,
            openai,
            CASE
                WHEN llm_model_name = 'openai/gpt-oss-120b' THEN
                    (COALESCE(token_count_prompt, 0) / 1000000.0) * 0.15
                    + (COALESCE(token_count_cache_read, 0) / 1000000.0) * 0.075
                    + (COALESCE(token_count_completion, 0) / 1000000.0) * 0.60
                ELSE NULL
            END AS cost_usd
        FROM iceberg_scan('{table.metadata_location}')
        WHERE start_time >= '{start_date}'
        ORDER BY start_time DESC
    """).df()
    return df
