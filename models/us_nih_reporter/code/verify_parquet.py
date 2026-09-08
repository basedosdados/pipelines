"""Verify the cleaned us_nih_reporter parquet before uploading.

Checks, over the whole cleaned corpus:

* every parquet column is STRING, in the architecture's order;
* row counts per table and per partition;
* the blank share of every column, which is what the architecture's documented
  percentages are measured from;
* the blank share within the most recent partition, which is what the dbt
  ``not_null_proportion_multiple_columns`` test actually sees once it is scoped
  to that partition;
* the logical key of each table is unique;
* the join keys hold: every ``core_project_num`` in the three link tables that
  is present in ``project``, and every ``project_abstract`` row that matches a
  ``project`` row on ``(year, application_id)``.

**Memory.** Blank counts come from the parquet row-group statistics, not from
the data — the writer turns every blank into a NULL, so ``null_count`` is
exactly the blank count, and reading it costs one metadata fetch per file
instead of materialising 8 GB of abstract text. Key and join checks do read
data, but only the two or three key columns, and they reduce each row to a
64-bit hash held in a numpy array rather than a Python set: 7.58M publication
link keys cost 60 MB that way and roughly 2 GB the obvious way. A hash
collision could in principle understate a duplicate count; at these sizes the
expected number is far below one, and any duplicate this reports should be
confirmed against the data before acting on it.

Run: uv run python models/us_nih_reporter/code/verify_parquet.py
"""

import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from common import ALL_TABLES, OUTPUT, assert_all_string, load_cols

KEYS = {
    "project": ["year", "application_id"],
    "project_abstract": ["year", "application_id"],
    "publication": ["year", "pmid"],
    "publication_link": ["year", "pmid", "core_project_num"],
    "patent_link": ["patent_id", "core_project_num", "patent_org_name"],
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


def null_counts(path: Path) -> tuple[int, dict[str, int]]:
    """Row count and per-column null count, read from the file's statistics.

    Raises when a column carries no statistics, rather than silently reporting
    zero blanks for it.
    """
    md = pq.ParquetFile(path).metadata
    names = md.schema.names
    out = dict.fromkeys(names, 0)
    for rg in range(md.num_row_groups):
        group = md.row_group(rg)
        for i, name in enumerate(names):
            stats = group.column(i).statistics
            if stats is None:
                raise AssertionError(
                    f"{path}: column {name} has no statistics"
                )
            out[name] += stats.null_count
    return md.num_rows, out


def key_hashes(
    table: str, files: list[Path], columns: list[str]
) -> np.ndarray:
    """64-bit hashes of one key per row, across every file of a table.

    ``np.int64`` rather than uint64: Python's ``hash`` returns a signed
    integer, and casting a negative one to unsigned is undefined by numpy's
    own warning.
    """
    chunks = []
    for f in files:
        d = pq.ParquetFile(f).read(columns=columns).to_pydict()
        cols = [d[c] for c in columns]
        chunks.append(
            np.fromiter(
                (hash(row) for row in zip(*cols, strict=True)),
                dtype=np.int64,
                count=len(cols[0]),
            )
        )
        del d, cols
    if not chunks:
        return np.empty(0, dtype=np.int64)
    return np.concatenate(chunks)


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

    print("\n=== rows and blanks (from parquet statistics) ===")
    for table in ALL_TABLES:
        fs = files_for(table)
        if not fs:
            continue
        cols = [c.name for c in load_cols(table)]
        total = 0
        blanks = Counter()
        per_year: dict[str, int] = {}
        for f in fs:
            n, nulls = null_counts(f)
            total += n
            blanks.update(nulls)
            part = f.parent.name
            if part.startswith("year="):
                per_year[part.split("=")[1]] = n
        print(f"\n-- {table}: {total:,} rows in {len(fs)} file(s)")
        if per_year:
            ys = sorted(per_year, key=int)
            print(f"   years {ys[0]}-{ys[-1]}, {len(ys)} partitions")
        for c in cols:
            share = 100 * blanks[c] / total if total else 0
            flag = "  <- below the 0.05 non-null floor" if share > 95 else ""
            print(f"   blank {c:<30} {blanks[c]:>10,} {share:6.2f}%{flag}")

    # The dbt not_null_proportion_multiple_columns test is scoped to the most
    # recent year on the partitioned tables (a full scan of 2.95M project rows
    # and 8 GB of abstract text on every run is not worth its cost), so the 0.05
    # non-null floor is evaluated against that partition alone. These are the
    # numbers the schema.yml ignore_values list must match.
    print(
        "\n=== blanks in the most recent partition (what the dbt test sees) ==="
    )
    for table in ALL_TABLES:
        parts = [
            f for f in files_for(table) if f.parent.name.startswith("year=")
        ]
        if not parts:
            continue
        newest = max(parts, key=lambda f: int(f.parent.name.split("=")[1]))
        n, nulls = null_counts(newest)
        over = [
            (c, 100 * nulls[c] / n)
            for c in [col.name for col in load_cols(table)]
            if n and 100 * nulls[c] / n > 95
        ]
        print(f"\n-- {table} {newest.parent.name}: {n:,} rows")
        if not over:
            print("   every column clears the floor")
        for c, share in over:
            print(f"   {c:<30} {share:6.2f}% blank  <- fails the 0.05 floor")

    print("\n=== key uniqueness ===")
    project_keys = None
    for table in ALL_TABLES:
        fs = files_for(table)
        if not fs:
            continue
        h = key_hashes(table, fs, KEYS[table])
        dupes = h.size - np.unique(h).size
        print(
            f"  {table:<22} key {KEYS[table]}: {dupes} duplicate(s) in {h.size:,}"
        )
        if dupes:
            problems.append(f"{table}: {dupes} duplicate keys")
        if table == "project":
            project_keys = np.unique(h)
        del h

    print("\n=== join integrity ===")
    cores = key_hashes("project", files_for("project"), ["core_project_num"])
    project_cores = np.unique(cores)
    del cores
    print(
        f"  project: {project_keys.size:,} keys, "
        f"{project_cores.size:,} distinct core_project_num"
    )

    for table in ("publication_link", "patent_link", "clinical_study_link"):
        fs = files_for(table)
        if not fs:
            continue
        h = key_hashes(table, fs, ["core_project_num"])
        hit = int(np.isin(h, project_cores, assume_unique=False).sum())
        print(
            f"  {table:<22} {hit:,}/{h.size:,} rows match a project "
            f"({100 * hit / h.size if h.size else 0:.2f}%)"
        )
        del h

    h = key_hashes(
        "project_abstract",
        files_for("project_abstract"),
        KEYS["project_abstract"],
    )
    hit = int(np.isin(h, project_keys, assume_unique=False).sum())
    print(
        f"  project_abstract       {hit:,}/{h.size:,} rows match a project "
        f"({100 * hit / h.size if h.size else 0:.2f}%)"
    )
    del h

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
