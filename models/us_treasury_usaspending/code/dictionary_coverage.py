"""Check which dictionary-covered columns the dicionario actually covers.

``custom_dictionary_coverage`` fails when a column holds a value the dictionary
does not define. The dicionario here is built from the DATA Act element
dictionary's published domain values, and that list is not always complete —
some FPDS code sets have grown values the published domain never listed. Rather
than assert coverage everywhere and watch it break, this measures it and emits
the list of columns that genuinely pass, for ``build_dbt.py --covered``.

Writes ``dictionary_coverage.json``:
    {"<table>": {"covered": [...], "gaps": {"<column>": ["<value>", ...]}}}

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/us_treasury_usaspending/code/dictionary_coverage.py
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from google.cloud import bigquery

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
DATASET = "us_treasury_usaspending"
TRANSACTION_TABLES = ("contract_transaction", "assistance_transaction")
MAX_GAP_SAMPLE = 8


def dictionary_columns(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open() as f:
        return [
            r["name"]
            for r in csv.DictReader(f)
            if r["covered_by_dictionary"] == "yes"
        ]


def analyse(client: bigquery.Client, project: str, table: str) -> dict:
    cols = dictionary_columns(table)
    if not cols:
        return {"covered": [], "gaps": {}}
    ref = f"`{project}.{DATASET}.{table}`"
    dic = f"`{project}.{DATASET}.dicionario`"
    unions = "\nunion all\n".join(
        f"""select '{c}' as column_name, t.{c} as value, count(*) as n
from {ref} t
left join (select chave from {dic} where nome_coluna = '{c}' and id_tabela = '{table}') d
  on d.chave = t.{c}
where t.{c} is not null and d.chave is null
group by 1, 2"""
        for c in cols
    )
    rows = list(
        client.query(f"select * from (\n{unions}\n) order by n desc").result()
    )

    gaps: dict[str, list[str]] = {}
    for r in rows:
        gaps.setdefault(r.column_name, [])
        if len(gaps[r.column_name]) < MAX_GAP_SAMPLE:
            gaps[r.column_name].append(f"{r.value} ({r.n:,})")
    covered = sorted(c for c in cols if c not in gaps)
    print(
        f"{table}: {len(covered)}/{len(cols)} dictionary columns fully covered"
    )
    for c, sample in sorted(gaps.items())[:10]:
        print(f"    gap {c}: {', '.join(sample[:4])}")
    if len(gaps) > 10:
        print(f"    ... and {len(gaps) - 10} more columns with gaps")
    return {"covered": covered, "gaps": gaps}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--tables", nargs="*", default=list(TRANSACTION_TABLES))
    ap.add_argument("--out", default=str(HERE / "dictionary_coverage.json"))
    args = ap.parse_args()

    client = bigquery.Client(project=args.project)
    out_path = Path(args.out)
    result = json.loads(out_path.read_text()) if out_path.exists() else {}
    for table in args.tables:
        result[table] = analyse(client, args.project, table)
    out_path.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
