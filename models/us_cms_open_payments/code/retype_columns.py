"""Correct the six mistyped count columns.

bulk_upsert_columns only writes bigquery_type when it CREATES a column
(`if not is_update:` in the MCP server), so an existing column's type cannot
be patched -- it has to be deleted and recreated. Column order is restored
afterwards, since the recreated columns land at the end.
"""

import sys

sys.path.insert(0, ".")
import constants as c
import layout
import register_metadata  # noqa: F401  imported for its retry wrappers
import server

TABLE = "summary_reporting_entity"
RETYPE = [
    "general_transaction_count_physician",
    "general_transaction_count_non_physician_practitioner",
    "general_transaction_count_teaching_hospital",
    "research_transaction_count_physician",
    "research_transaction_count_non_physician_practitioner",
    "research_transaction_count_teaching_hospital",
]

payload = (c.ARCH_DIR / "payloads" / f"{TABLE}.json").read_text()
for env in ("staging", "prod"):
    print(f"=== {env}", flush=True)
    snapshot = server.get_dataset(slug="open_payments", env=env)
    table = snapshot["tables"][TABLE]
    ids = {col["name"]: col["id"] for col in table["columns"]}
    for name in RETYPE:
        server.delete_column(column_id=ids[name], env=env)
        print(f"  deleted {name}", flush=True)
    out = server.bulk_upsert_columns(
        table_id=table["id"], columns_json=payload, env=env
    )
    print(
        f"  recreated: created={out['created']} updated={out['updated']} errors={out['errors']}",
        flush=True,
    )
    server.reorder_columns(
        table_id=table["id"], column_names=layout.LAYOUT[TABLE], env=env
    )
    print("  column order restored", flush=True)
print("DONE", flush=True)
