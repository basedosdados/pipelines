"""Work out which half of each code/label pair actually holds the code.

The archive is not consistent about this. In the Assistance files the
``<x>_code`` column holds the code, as the name promises. In the Contracts
files several pairs are **inverted**: ``action_type_code`` holds
"OTHER ADMINISTRATIVE ACTION" while ``action_type`` holds ``M``. Naming cannot
be trusted, so orientation is measured: for each column of a pair, compare its
distinct values against the DATA Act domain's keys and against its labels, and
let the better overlap decide.

Writes ``code_label_orientation.json``:
    {"<table>": {"<pair>": {"code": "<column>", "label": "<column>",
                            "key_hits": .., "label_hits": ..}}}

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/us_treasury_usaspending/code/code_label_orientation.py
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from google.cloud import bigquery

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
sys.path.insert(0, str(ARCH))

from build_architecture import (  # noqa: E402
    enumerated_domain,
    load_data_dictionary,
)
from descriptions import CODE_PAIRS  # noqa: E402

DATASET = "us_treasury_usaspending"
TRANSACTION_TABLES = ("contract_transaction", "assistance_transaction")
SAMPLE_VALUES = 200


def table_columns(table: str) -> set[str]:
    with (ARCH / f"{table}.csv").open() as f:
        return {r["name"] for r in csv.DictReader(f)}


def distinct_values(
    client: bigquery.Client, project: str, table: str, col: str
) -> set[str]:
    q = f"""
    select {col} as v
    from `{project}.{DATASET}.{table}`
    where {col} is not null
    group by 1
    order by count(*) desc
    limit {SAMPLE_VALUES}
    """
    return {str(r.v) for r in client.query(q).result()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--out", default=str(HERE / "code_label_orientation.json"))
    args = ap.parse_args()

    client = bigquery.Client(project=args.project)
    ddict = load_data_dictionary()
    result: dict[str, dict] = {}

    for table in TRANSACTION_TABLES:
        present = table_columns(table)
        result[table] = {}
        for code_col, (label_cols, *_rest) in CODE_PAIRS.items():
            partners = [c for c in label_cols if c in present]
            if code_col not in present or not partners:
                continue
            domain = enumerated_domain(
                ddict.get(code_col, {}).get("domain", "")
            )
            if not domain:
                continue
            keys = {k.upper() for k, _ in domain}
            labels = {v.upper() for _, v in domain}
            partner = partners[0]

            scored = {}
            for col in (code_col, partner):
                vals = {
                    v.upper()
                    for v in distinct_values(client, args.project, table, col)
                }
                if not vals:
                    continue
                scored[col] = {
                    "key_hits": len(vals & keys) / len(vals),
                    "label_hits": len(vals & labels) / len(vals),
                }
            if len(scored) != 2:
                continue

            # The column whose values look more like domain keys than like
            # domain labels, relative to the other column, is the code.
            def margin(c: str) -> float:
                return scored[c]["key_hits"] - scored[c]["label_hits"]

            code = max(scored, key=margin)
            label = partner if code == code_col else code_col
            result[table][code_col] = {
                "code": code,
                "label": label,
                "inverted": code != code_col,
                **{f"{c}_scores": scored[c] for c in scored},
            }
            flag = "  INVERTED" if code != code_col else ""
            print(
                f"{table}: {code_col} -> code={code} "
                f"(key {scored[code]['key_hits']:.2f} / label {scored[code]['label_hits']:.2f}){flag}"
            )

    Path(args.out).write_text(
        json.dumps(result, indent=1, sort_keys=True) + "\n"
    )
    inverted = sum(
        1 for t in result.values() for v in t.values() if v["inverted"]
    )
    print(f"\n{inverted} inverted pair(s); wrote {args.out}")


if __name__ == "__main__":
    main()
