"""Verify the cleaned us_nih_reporter parquet before uploading.

Checks, over the whole cleaned corpus:

* every parquet column is STRING, in the architecture's order;
* row counts per table and per partition;
* the blank share of every column, which is what the architecture's documented
  percentages are measured from;
* the logical key of each table is unique;
* the join keys hold: every ``core_project_num`` in the three link tables that
  is present in ``project``, and every ``project_abstract`` row that matches a
  ``project`` row on ``(year, application_id)``.

Run: uv run python models/us_nih_reporter/code/verify_parquet.py
"""

import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from common import ALL_TABLES, OUTPUT, assert_all_string, load_cols

KEYS = {
    "project": ["year", "application_id"],
    "project_abstract": ["year", "application_id"],
    "publication": ["year", "pmid"],
    "publication_link": ["year", "pmid", "core_project_num"],
    "patent_link": ["patent_id", "core_project_num"],
    "clinical_study_link": ["nct_id", "core_project_num"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}


def files_for(table: str) -> list[Path]:
    return sorted((OUTPUT / table).rglob("*.parquet"))


def check_schema(table: str) -> None:
    cols = [c.name for c in load_cols(table)]
    for f in files_for(table):
        schema = pq.read_schema(f)
        if list(schema.names) != cols:
            raise AssertionError(
                f"{f}: column order {list(schema.names)} != architecture {cols}"
            )


def main() -> int:
    problems = []
    print("=== schema ===")
    for table in ALL_TABLES:
        if not files_for(table):
            problems.append(f"{table}: no parquet written")
            continue
        check_schema(table)
        assert_all_string(OUTPUT / table)
        print(f"  {table:<22} all-STRING, architecture order")

    print("\n=== rows, blanks and keys ===")
    for table in ALL_TABLES:
        fs = files_for(table)
        if not fs:
            continue
        cols = [c.name for c in load_cols(table)]
        total = 0
        blanks = Counter()
        keyset: set[tuple] = set()
        dupes = 0
        per_year: dict[str, int] = {}
        for f in fs:
            t = pq.ParquetFile(f).read()
            n = t.num_rows
            total += n
            part = f.parent.name
            if part.startswith("year="):
                per_year[part.split("=")[1]] = n
            d = t.to_pydict()
            for c in cols:
                blanks[c] += sum(1 for v in d[c] if v is None or v == "")
            kcols = KEYS[table]
            for row in zip(*(d[k] for k in kcols), strict=True):
                if row in keyset:
                    dupes += 1
                else:
                    keyset.add(row)
        print(f"\n-- {table}: {total:,} rows in {len(fs)} file(s)")
        if per_year:
            ys = sorted(per_year, key=int)
            print(f"   years {ys[0]}-{ys[-1]}, {len(ys)} partitions")
        print(f"   key {KEYS[table]}: {dupes} duplicate(s)")
        if dupes:
            problems.append(f"{table}: {dupes} duplicate keys")
        for c in cols:
            share = 100 * blanks[c] / total if total else 0
            flag = "  <- below the 0.05 non-null floor" if share > 95 else ""
            print(f"   blank {c:<30} {blanks[c]:>10,} {share:6.2f}%{flag}")

    print("\n=== join integrity ===")
    project_cores: set[str] = set()
    project_keys: set[tuple] = set()
    for f in files_for("project"):
        d = (
            pq.ParquetFile(f)
            .read(columns=["year", "application_id", "core_project_num"])
            .to_pydict()
        )
        project_cores.update(v for v in d["core_project_num"] if v)
        project_keys.update(zip(d["year"], d["application_id"], strict=True))
    print(
        f"  project: {len(project_keys):,} keys, {len(project_cores):,} distinct core_project_num"
    )

    for table in ("publication_link", "patent_link", "clinical_study_link"):
        fs = files_for(table)
        if not fs:
            continue
        n = 0
        hit = 0
        missing = Counter()
        for f in fs:
            d = (
                pq.ParquetFile(f)
                .read(columns=["core_project_num"])
                .to_pydict()
            )
            for v in d["core_project_num"]:
                n += 1
                if v in project_cores:
                    hit += 1
                elif v:
                    missing[v] += 1
        print(
            f"  {table:<22} {hit:,}/{n:,} rows match a project "
            f"({100 * hit / n if n else 0:.2f}%), "
            f"{len(missing):,} distinct unmatched core_project_num"
        )
        if missing:
            print(f"      e.g. {[k for k, _ in missing.most_common(5)]}")

    n = 0
    hit = 0
    for f in files_for("project_abstract"):
        d = (
            pq.ParquetFile(f)
            .read(columns=["year", "application_id"])
            .to_pydict()
        )
        for k in zip(d["year"], d["application_id"], strict=True):
            n += 1
            if k in project_keys:
                hit += 1
    print(
        f"  project_abstract       {hit:,}/{n:,} rows match a project "
        f"({100 * hit / n if n else 0:.2f}%)"
    )

    print("\n=== dicionario coverage ===")
    dic: dict[tuple[str, str], set[str]] = defaultdict(set)
    labelled: dict[tuple[str, str], set[str]] = defaultdict(set)
    for f in files_for("dicionario"):
        d = pq.ParquetFile(f).read().to_pydict()
        for tbl, col, key, val in zip(
            d["id_tabela"],
            d["nome_coluna"],
            d["chave"],
            d["valor"],
            strict=True,
        ):
            dic[(tbl, col)].add(key)
            if val:
                labelled[(tbl, col)].add(key)
    for k in sorted(dic):
        print(
            f"  {k[0]}.{k[1]:<20} {len(dic[k]):>5} value(s), "
            f"{len(labelled[k]):>5} with a label"
        )

    print("\n=== verdict ===")
    if problems:
        for p in problems:
            print(f"  FAIL {p}")
        return 1
    print("  PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
