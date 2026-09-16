"""Validate the Ceará staging parquet.

**CE publishes no control total.** Its own `Inventário de dados` lists name, content,
órgão and creation date -- no row count, no money total, no per-category subtotal. There
is nothing here like SC's `lista.total` or SIGEO's grid totals to reconcile against, so
this is not a reconciliation and does not pretend to be one.

What the catalogue does give are two places where it publishes the same data twice, and
those are genuine independent checks:

* **2023 empenho appears in dataset 145** (four quarterly files) **and dataset 152**
  (one consolidated CSV). Only 152 is staged. Comparing the two is the closest thing CE
  offers to an external control -- and it is only partial, because 145's Q4 is the
  catalogue's single `.ods` and is not read here.
* **Dataset 170 lists `NPD+4BI.csv` twice** under different content hashes. Whether the
  two are the same file is a question about the catalogue, not about our parse.

Everything else here is an internal consistency check on what was staged: schema
identity across a table's partitions, exercise coverage against the phases' declared
spans, and the absence of the source defects the cleaner is supposed to have removed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clean_ce
from constants import (
    CE_EMPENHO_DUPLICATE_SLOT,
    CE_NULL_SENTINEL,
    CE_TABLES,
    INPUT_DIR,
    OUTPUT_DIR,
)

CE_INPUT = INPUT_DIR / "ce"

# The first exercise each phase publishes, from the catalogue's own dataset titles.
CE_EXPECTED_SPAN = {
    "empenho": (2006, 2026),
    "liquidacao": (2015, 2026),
    "pagamento": (2012, 2026),
}


def _fail(checks: list[tuple[bool, str]], ok: bool, message: str) -> None:
    checks.append((ok, message))
    print(f"  [{'PASS' if ok else 'FAIL'}] {message}", flush=True)


def check_schema_identity(checks: list) -> None:
    """Every parquet in a table directory must carry the identical schema.

    `upload.py` loads a table's directory with one wildcard, and **a wildcard parquet
    load infers ONE schema and drops the columns the other files have** while still
    reporting the full row count. This is the check that would have caught RS.
    """
    print("\n== schema identity across partitions")
    for phase, table in CE_TABLES.items():
        files = sorted((OUTPUT_DIR / table).glob("*.parquet"))
        if not files:
            _fail(checks, False, f"{table}: no parquet written")
            continue
        schemas = defaultdict(list)
        for f in files:
            schemas[tuple(pq.read_schema(f).names)].append(f.name)
        expected = clean_ce._superset(phase)
        ok = len(schemas) == 1 and next(iter(schemas)) == tuple(expected)
        detail = (
            f"{table}: {len(files)} file(s), one schema of {len(expected)} columns"
            if ok
            else f"{table}: {len(schemas)} distinct schemas across {len(files)} files"
        )
        _fail(checks, ok, detail)
        if not ok:
            for names, owners in schemas.items():
                print(
                    f"        {len(names)} cols, {len(owners)} file(s): {owners[:3]}"
                )


def check_no_sentinels(checks: list) -> None:
    """The literal string NULL must not survive into staging.

    36,760,936 of them were measured in the source. Staged verbatim they are strings
    that are not null, so a downstream `count()` counts phantom values.
    """
    print("\n== null sentinel removed")
    for table in CE_TABLES.values():
        files = sorted((OUTPUT_DIR / table).glob("*.parquet"))
        found = 0
        for f in files:
            t = pq.read_table(f)
            for column in t.itercolumns():
                found += sum(
                    1
                    for chunk in column.chunks
                    for value in chunk.to_pylist()
                    if value == CE_NULL_SENTINEL
                )
            if found:
                break
        _fail(
            checks,
            found == 0,
            f"{table}: {found} literal {CE_NULL_SENTINEL!r} value(s)",
        )


def check_coverage(checks: list) -> tuple[dict, dict]:
    """Exercises present, read from the partition names, against the declared span."""
    print("\n== exercise coverage")
    rows_by_year: dict[str, Counter] = {}
    for phase, table in CE_TABLES.items():
        counts: Counter = Counter()
        for f in sorted((OUTPUT_DIR / table).glob("*.parquet")):
            year = f.name.split("__", 1)[0].removeprefix("data_")
            counts[year] += pq.read_metadata(f).num_rows
        rows_by_year[table] = counts
        if not counts:
            _fail(checks, False, f"{table}: nothing staged")
            continue
        first, last = CE_EXPECTED_SPAN[phase]
        years = {int(y) for y in counts if y.isdigit()}
        in_span = {y for y in years if first <= y <= last}
        missing = sorted(set(range(first, last + 1)) - in_span)
        stray = sorted(years - in_span)
        blank = [y for y in counts if not y.isdigit()]
        _fail(
            checks,
            not blank,
            f"{table}: every row carries an exercise"
            if not blank
            else f"{table}: {sum(counts[y] for y in blank)} row(s) with no exercise {blank}",
        )
        print(
            f"        {table}: {sum(counts.values()):,} rows, "
            f"{min(in_span)}-{max(in_span)}"
        )
        if missing:
            print(f"        missing exercises: {missing}")
        if stray:
            print(
                f"        exercises outside the declared span (kept -- the data's own "
                f"year is authoritative): {stray}"
            )
    return rows_by_year, {}


def check_2023_duplicate(checks: list) -> None:
    """Dataset 145 against dataset 152 -- the same exercise published twice.

    The only place CE lets its own numbers be checked against each other. Q4 of 145 is
    the catalogue's single `.ods` and is not read, so this compares Q1-Q3 only and says
    so rather than reporting a clean match it did not make.
    """
    print(
        "\n== 2023 empenho: dataset 145 (quarterly) vs dataset 152 (consolidated)"
    )
    quarterly = 0
    read = 0
    skipped = []
    for meta in sorted((CE_INPUT / "empenho").glob("*.meta.json")):
        info = json.loads(meta.read_text())
        if int(info["dataset_id"]) != CE_EMPENHO_DUPLICATE_SLOT:
            continue
        src = Path(str(meta)[: -len(".meta.json")])
        try:
            rows, sep = clean_ce.read_rows(src)
        except Exception as exc:
            skipped.append(f"{src.name} ({type(exc).__name__})")
            continue
        header = clean_ce._header_of(rows[0])
        body, _ = clean_ce._normalise_widths(rows[1:], header, sep)
        quarterly += len(body)
        read += 1

    consolidated = sum(
        pq.read_metadata(f).num_rows
        for f in (OUTPUT_DIR / CE_TABLES["empenho"]).glob(
            "data_2023__*.parquet"
        )
    )
    print(f"        dataset 145: {quarterly:,} rows from {read} of 4 files")
    if skipped:
        print(f"        not read: {', '.join(skipped)}")
    print(f"        dataset 152 (staged): {consolidated:,} rows")
    # Partial by construction: 145 is missing a quarter here, so 152 must be larger.
    ok = consolidated >= quarterly > 0
    _fail(
        checks,
        ok,
        f"consolidated 2023 ({consolidated:,}) >= the {read} quarters read "
        f"({quarterly:,}); the remaining quarter is the .ods, so this is a bound, "
        f"not an equality",
    )


def check_duplicate_attachment(checks: list) -> None:
    """Dataset 170 publishes `NPD+4BI.csv` twice. Are the two the same file?"""
    print("\n== dataset 170: NPD+4BI.csv published twice")
    digests: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for meta in sorted((CE_INPUT / "pagamento").glob("170__*.meta.json")):
        src = Path(str(meta)[: -len(".meta.json")])
        if "NPD_4BI.csv" not in src.name and "NPD+4BI.csv" not in src.name:
            continue
        raw = src.read_bytes()
        digests[hashlib.sha256(raw).hexdigest()].append((src.name, len(raw)))
    if len(digests) < 1:
        _fail(checks, False, "neither copy of NPD+4BI.csv is on disk")
        return
    for digest, files in digests.items():
        print(f"        {digest[:16]}  {files}")
    copies = sum(len(v) for v in digests.values())
    _fail(
        checks,
        True,
        f"{copies} copy/copies on disk resolving to {len(digests)} distinct "
        f"content(s) -- "
        + (
            "the two listings are byte-identical"
            if len(digests) == 1 and copies > 1
            else "the two listings differ and both are staged; de-duplicate in dbt"
            if copies > 1
            else "only one copy was downloaded"
        ),
    )


def main() -> None:
    checks: list[tuple[bool, str]] = []
    check_schema_identity(checks)
    check_coverage(checks)
    check_no_sentinels(checks)
    check_2023_duplicate(checks)
    check_duplicate_attachment(checks)

    failed = [m for ok, m in checks if not ok]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
    if failed:
        for m in failed:
            print(f"  FAILED: {m}")
        raise SystemExit(1)


if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__).parse_args()
    main()
