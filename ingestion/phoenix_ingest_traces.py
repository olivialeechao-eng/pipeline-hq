import json
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import GreaterThanOrEqual
from phoenix.client import Client
from dotenv import load_dotenv

load_dotenv()

BASE_DIR      = Path(__file__).resolve().parent.parent
ICEBERG_DIR   = BASE_DIR / "data" / "iceberg"
CATALOG_URI   = f"sqlite:///{ICEBERG_DIR}/catalog.db"
WAREHOUSE_URI = f"file://{ICEBERG_DIR}/warehouse"

ICEBERG_DIR.mkdir(parents=True, exist_ok=True)

TABLE_ID = ("raw_phoenix", "agent_spans")

SCHEMA = pa.schema([
    pa.field("span_id",                     pa.string()),
    pa.field("trace_id",                    pa.string()),
    pa.field("name",                        pa.string()),
    pa.field("span_kind",                   pa.string()),
    pa.field("start_time",                  pa.timestamp("us", tz="UTC")),
    pa.field("end_time",                    pa.timestamp("us", tz="UTC")),
    pa.field("status_code",                 pa.string()),
    pa.field("status_message",              pa.string()),
    pa.field("parent_id",                   pa.string()),
    pa.field("events",                      pa.string()),  # JSON
    pa.field("llm_model_name",              pa.string()),
    pa.field("llm_provider",                pa.string()),
    pa.field("llm_system",                  pa.string()),
    pa.field("llm_finish_reason",           pa.string()),
    pa.field("llm_invocation_parameters",   pa.string()),  # JSON
    pa.field("llm_input_messages",          pa.string()),  # JSON
    pa.field("llm_output_messages",         pa.string()),  # JSON
    pa.field("token_count_total",           pa.int64()),
    pa.field("token_count_prompt",          pa.int64()),
    pa.field("token_count_cache_read",      pa.int64()),
    pa.field("token_count_completion",      pa.int64()),
    pa.field("output_value",                pa.string()),
    pa.field("output_mime_type",            pa.string()),
    pa.field("openinference_span_kind",     pa.string()),
    pa.field("gen_ai",                      pa.string()),  # JSON
    pa.field("openai",                      pa.string()),  # JSON
    pa.field("ingested_at",                 pa.string()),
])

# Iceberg field names that hold nested objects serialized as JSON
_JSON_COLS = {
    "events", "llm_invocation_parameters", "llm_input_messages",
    "llm_output_messages", "gen_ai", "openai",
}

_COL_MAP = {
    "context.span_id":                                      "span_id",
    "context.trace_id":                                     "trace_id",
    "name":                                                  "name",
    "span_kind":                                             "span_kind",
    "start_time":                                            "start_time",
    "end_time":                                              "end_time",
    "status_code":                                           "status_code",
    "status_message":                                        "status_message",
    "parent_id":                                             "parent_id",
    "events":                                                "events",
    "attributes.llm.model_name":                             "llm_model_name",
    "attributes.llm.provider":                               "llm_provider",
    "attributes.llm.system":                                 "llm_system",
    "attributes.llm.finish_reason":                          "llm_finish_reason",
    "attributes.llm.invocation_parameters":                  "llm_invocation_parameters",
    "attributes.llm.input_messages":                         "llm_input_messages",
    "attributes.llm.output_messages":                        "llm_output_messages",
    "attributes.llm.token_count.total":                      "token_count_total",
    "attributes.llm.token_count.prompt":                     "token_count_prompt",
    "attributes.llm.token_count.prompt_details.cache_read":  "token_count_cache_read",
    "attributes.llm.token_count.completion":                 "token_count_completion",
    "attributes.output.value":                               "output_value",
    "attributes.output.mime_type":                           "output_mime_type",
    "attributes.openinference.span.kind":                    "openinference_span_kind",
    "attributes.gen_ai":                                     "gen_ai",
    "attributes.openai":                                     "openai",
}


def _get_catalog() -> SqlCatalog:
    return SqlCatalog("local", uri=CATALOG_URI, warehouse=WAREHOUSE_URI)


def load_agent_spans_table():
    return _get_catalog().load_table(TABLE_ID)

def ingest(start_date: datetime | None = None, limit: int = 1000) -> int:
    client = Client(
        base_url=os.environ["PHOENIX_BASE_URL"],
        api_key=os.environ["PHOENIX_API_KEY"],
    )
    if start_date is None:
        start_date = datetime.now(timezone.utc) - timedelta(days=1)
    elif not isinstance(start_date, datetime):
        # Handle pandas Timestamp or date objects
        start_date = pd.Timestamp(start_date).to_pydatetime().replace(tzinfo=timezone.utc)
    df = client.spans.get_spans_dataframe(
        project_identifier="pipeline-hq",
        root_spans_only=False, #includes full trace
        start_time=start_date,
        limit=limit,
    )

    if df.empty:
        print("No spans returned from Phoenix")
        return 0

    ingested_at = datetime.now(timezone.utc).isoformat()

    present_cols = {src: dst for src, dst in _COL_MAP.items() if src in df.columns}
    df = df[list(present_cols.keys())].rename(columns=present_cols)

    for field in SCHEMA:
        if field.name not in df.columns:
            df[field.name] = None

    df["ingested_at"] = ingested_at

    for col in _JSON_COLS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else json.dumps(v) if not isinstance(v, str) else v)

    for col in ["start_time", "end_time"]:
        if col in df.columns and hasattr(df[col].dtype, "tz"):
            df[col] = df[col].dt.as_unit("us")

    arrow_table = pa.Table.from_pandas(df[SCHEMA.names], schema=SCHEMA, preserve_index=False)

    catalog = _get_catalog()
    catalog.create_namespace_if_not_exists("raw_phoenix")

    if catalog.table_exists(TABLE_ID):
        table = catalog.load_table(TABLE_ID)
        table.overwrite(arrow_table, overwrite_filter=GreaterThanOrEqual("start_time", start_date.isoformat()))  # type: ignore[call-arg]
        print(f"Upserted {len(arrow_table)} spans")
    else:
        table = catalog.create_table(TABLE_ID, schema=SCHEMA)
        table.append(arrow_table)
        print(f"Created raw_phoenix.agent_spans and wrote {len(arrow_table)} spans")

    count = table.scan().count()
    print(f"Total rows: {count}")
    return len(arrow_table)


if __name__ == "__main__":
    ingest()
