"""Ask whether a table's repeated keys are revisions or distinct records.

Every duplicate-key tolerance in this dataset turned out to be covering a defect,
and in three of five cases the defect was the same one: the source revises a row
and re-serves it in a later window, so both states survive and any sum over the
table double counts. This makes that diagnosis one command instead of hand-written
SQL, so it is not skipped on the tables that land last.

Read the output as: if the repeats differ only in a status flag, a timestamp, or a
field that fills in later (a supplier appearing once a bid is awarded), they are
revisions -- set `dedup_by_key` and drop the tolerance. If they differ in what the
record *is* -- a different object, value, or counterparty -- they are distinct
records that must not be collapsed.

    uv run python models/br_mgi_compras_publicas/code/check_key.py contrato_item
"""

from __future__ import annotations

import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dbt_spec import TABLES

DATASET = "basedosdados-dev.br_mgi_compras_publicas"


def _client() -> bigquery.Client:
    try:
        import tomllib as toml_reader
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as toml_reader

    cfg = toml_reader.loads(
        (Path.home() / ".basedosdados" / "config.toml").read_text()
    )
    cred = service_account.Credentials.from_service_account_file(
        cfg["gcloud-projects"]["staging"]["credentials_path"]
    )
    return bigquery.Client(credentials=cred, project=cred.project_id)


def report(table: str) -> None:
    spec = TABLES[table]
    client = _client()
    schema = client.get_table(f"{DATASET}.{table}").schema
    key = ", ".join(spec.key)
    # Compare every column that is not part of the key. STRUCT is null-safe,
    # unlike FORMAT, which returns NULL on a NULL argument -- that silently drops
    # rows from COUNT(DISTINCT) and once made a five-column key look less
    # selective than the four-column key it contains.
    others = [f.name for f in schema if f.name not in spec.key]
    counts = ",\n    ".join(
        f"count(distinct cast({c} as string)) as n_{c}" for c in others
    )
    varies = ",\n    ".join(f"countif(n_{c} > 1) as {c}" for c in others)
    # A table whose key is legitimately absent on some rows keeps those rows by
    # partitioning them on their own content, and its dbt test is scoped to the
    # rows that have a key. Report on the same population the test does: lumping
    # the two together makes the expected fallback rows look like a defect and,
    # worse, would let a real defect hide among them.
    scope = spec.unique_where or (
        " and ".join(f"{k} is not null" for k in spec.key)
        if spec.dedup_key_nullable
        else ""
    )
    where = f"where {scope}" if scope else ""
    sql = f"""
with t as (
  select *, to_json_string(struct({key})) k from `{DATASET}.{table}` {where}
),
d as (select k from t group by k having count(*) > 1),
per_key as (
  select k,
    {counts}
  from t where k in (select k from d) group by k
)
select (select count(*) from t) as rows_total,
       (select count(*) from d) as dup_keys,
       {varies}
from per_key
"""
    row = dict(next(iter(client.query(sql).result())))
    total, dups = row.pop("rows_total"), row.pop("dup_keys")
    print(
        f"{table}: {dups:,} duplicate keys in {total:,} rows "
        f"({100 * dups / max(total, 1):.4f}%)  key = [{key}]"
    )
    if scope:
        excluded = next(
            iter(
                client.query(
                    f"select count(*) n from `{DATASET}.{table}` "
                    f"where not ({scope})"
                ).result()
            )
        )["n"]
        print(f"  counted over: {scope}")
        print(
            f"  ({excluded:,} further rows have no complete key and are kept on "
            "their own content, by design)"
        )
    if not dups:
        print("  no repeated keys")
        return
    for col, n in sorted(row.items(), key=lambda kv: -kv[1]):
        if n:
            print(f"  {100 * n / dups:5.1f}%  differ in {col}")


if __name__ == "__main__":
    for name in sys.argv[1:] or sorted(TABLES):
        report(name)
