"""Staging parquet must carry every column the dbt model reads.

This exists because of a real failure. The architecture renamed eight columns
per table after the onboarding upload (`*_unique_key` -> `*_id`, the four
`*_fips_code` -> `*_id`, `IIJA` -> `iija`), and the dbt model kept reading the
source spellings from staging. The cleaner projected onto the architecture
names, found no such columns in the source, and wrote them as NULL, so the
2026-10-13 dev run produced a fiscal year with no transaction or award ids.

Nothing caught it: every column was present, just empty, and the dbt tests
read only the newest fiscal year's null proportions against exemptions measured
on the older, correct files.
"""

import csv
import json
import re
from pathlib import Path

import pytest

from pipelines.datasets.us_treasury_usaspending.utils import (
    RENAMES,
    staging_columns,
)

MODELS = (
    Path(__file__).resolve().parents[4] / "models" / "us_treasury_usaspending"
)


def _staging_reads(table: str) -> set[str]:
    """Source column names the model mentions anywhere, lowercased.

    Matching on the architecture's ``original_name`` rather than on the shape
    of the SQL catches the columns read through nested expressions too (the
    county FIPS repair wraps its input in ``lpad``/``cast``), and ignores the
    output aliases, which carry the published names.
    """
    sql = (MODELS / f"us_treasury_usaspending__{table}.sql").read_text()
    words = set(re.findall(r"[a-z0-9_]+", re.sub(r"--.*", "", sql).lower()))
    arch = MODELS / "code" / "architecture" / f"{table}.csv"
    with arch.open() as f:
        sources = {row["original_name"].lower() for row in csv.DictReader(f)}
    return words & sources


@pytest.mark.parametrize(
    "table", ["contract_transaction", "assistance_transaction"]
)
def test_every_column_dbt_reads_is_in_staging(table: str) -> None:
    staged = {c.lower() for c in staging_columns(table)}
    missing = sorted(_staging_reads(table) - staged)
    assert not missing, f"dbt reads columns staging lacks: {missing}"


@pytest.mark.parametrize(
    "table", ["contract_transaction", "assistance_transaction"]
)
def test_staging_names_are_bigquery_safe(table: str) -> None:
    bad = [
        c
        for c in staging_columns(table)
        if not re.fullmatch(r"[A-Za-z_]\w*", c)
    ]
    assert not bad, f"rename these in RENAMES: {bad}"
    assert set(RENAMES.values()) & set(staging_columns(table))


@pytest.mark.parametrize(
    ("table", "family"),
    [
        ("contract_transaction", "contracts"),
        ("assistance_transaction", "assistance"),
    ],
)
def test_staging_columns_match_source_headers_exactly(
    table: str, family: str
) -> None:
    """Case-sensitively: the cleaner matches source columns by exact name.

    `outlayed_amount_from_iija_...` against a source header spelled `IIJA`
    finds nothing and is written as NULL.
    """
    headers = json.loads(
        (MODELS / "code" / "architecture" / "source_headers.json").read_text()
    )[family]
    renamed = {RENAMES.get(h, h) for h in headers}
    missing = [c for c in staging_columns(table) if c not in renamed]
    assert not missing, f"staging columns absent from the source: {missing}"
