# https://docs.github.com/en/rest/pulls/pulls?apiVersion=2026-03-10#get-a-pull-request

from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In

BASE_DIR      = Path(__file__).resolve().parent.parent
ICEBERG_DIR   = BASE_DIR / "data" / "iceberg"
CATALOG_URI   = f"sqlite:///{ICEBERG_DIR}/catalog.db"
WAREHOUSE_URI = f"file://{ICEBERG_DIR}/warehouse"

ICEBERG_DIR.mkdir(parents=True, exist_ok=True)

TABLE_ID = ("raw_github", "pr_metadata")

_ts = pa.timestamp("us", tz="UTC")

SCHEMA = pa.schema([
    pa.field("pr_id",           pa.int64()),
    pa.field("pr_number",       pa.int32()),
    pa.field("state",           pa.string()),
    pa.field("merged",          pa.bool_()),
    pa.field("merged_at",       _ts),
    pa.field("closed_at",       _ts),
    pa.field("created_at",      _ts),
    pa.field("updated_at",      _ts),
    pa.field("title",           pa.string()),
    pa.field("author",          pa.string()),
    pa.field("additions",       pa.int32()),
    pa.field("deletions",       pa.int32()),
    pa.field("changed_files",   pa.int32()),
    pa.field("commits",         pa.int32()),
    pa.field("review_comments", pa.int32()),
    pa.field("comments",        pa.int32()),
    pa.field("ingested_at",     _ts),
])


def _parse_ts(s: str | None) -> datetime | None:
    if s is None:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _flatten_pr(pr: dict, ingested_at: datetime) -> dict:
    return {
        "pr_id":           pr["id"],
        "pr_number":       pr["number"],
        "state":           pr["state"],
        "merged":          pr["merged"],
        "merged_at":       _parse_ts(pr.get("merged_at")),
        "closed_at":       _parse_ts(pr.get("closed_at")),
        "created_at":      _parse_ts(pr["created_at"]),
        "updated_at":      _parse_ts(pr["updated_at"]),
        "title":           pr["title"],
        "author":          pr["user"]["login"],
        "additions":       pr.get("additions", 0),
        "deletions":       pr.get("deletions", 0),
        "changed_files":   pr.get("changed_files", 0),
        "commits":         pr.get("commits", 0),
        "review_comments": pr.get("review_comments", 0),
        "comments":        pr.get("comments", 0),
        "ingested_at":     ingested_at,
    }


def _get_catalog() -> SqlCatalog:
    return SqlCatalog("local", uri=CATALOG_URI, warehouse=WAREHOUSE_URI)


def load_pr_metadata_table():
    return _get_catalog().load_table(TABLE_ID)


def ingest(prs: list[dict]) -> int:
    ingested_at = datetime.now(timezone.utc)
    rows = [_flatten_pr(pr, ingested_at) for pr in prs]
    pr_ids = [r["pr_id"] for r in rows]

    arrow_table = pa.Table.from_pylist(rows, schema=SCHEMA)

    catalog = _get_catalog()
    catalog.create_namespace_if_not_exists("raw_github")

    if catalog.table_exists(TABLE_ID):
        table = catalog.load_table(TABLE_ID)
        table.overwrite(arrow_table, overwrite_filter=In("pr_id", pr_ids))  # type: ignore[call-arg]
        print(f"Upserted {len(rows)} row(s)")
    else:
        table = catalog.create_table(TABLE_ID, schema=SCHEMA)
        table.append(arrow_table)
        print(f"Created raw_github.pr_metadata and wrote {len(rows)} row(s)")

    count = table.scan().count()
    print(f"Total rows: {count}")
    return len(rows)


if __name__ == "__main__":
    raw_pr = {
        "id": 1,
        "number": 1347,
        "state": "open",
        "merged": True,
        "merged_at": "2011-01-26T19:01:12Z",
        "closed_at": "2011-01-26T19:01:12Z",
        "created_at": "2011-01-26T19:01:12Z",
        "updated_at": "2011-01-26T19:01:12Z",
        "title": "Amazing new feature",
        "user": {"login": "octocat"},
        "additions": 100,
        "deletions": 3,
        "changed_files": 5,
        "commits": 3,
        "review_comments": 0,
        "comments": 10,
    }
    ingest([raw_pr])
