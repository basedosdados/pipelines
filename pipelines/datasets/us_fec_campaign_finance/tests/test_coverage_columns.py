"""Every coverage spec must name a column the table actually has.

This exists because of a real failure. The partition was renamed `cycle` -> `year`
across the architecture, the dbt models, the parquet and the backend, but the three
`AllFree` specs in flows.py kept `YearOnly(col="cycle")`. Nothing caught it:

- `pyrefly` cannot: "cycle" is a valid string.
- The dev validation run cannot: it is triggered with `update_metadata=False`, which
  skips `register_table_materialization_task` — the only caller that reads these
  columns. The whole dev run went green with the bug in place.
- It surfaced only on the prod run, as
  `400 ... Unrecognized name: cycle`, from `MAX(DATE(cycle,1,1))`.

The architecture CSVs are the source of truth for column names, so comparing the
specs against them closes the gap without needing BigQuery.
"""

import pytest

from pipelines.datasets.us_fec_campaign_finance.constants import constants
from pipelines.datasets.us_fec_campaign_finance.flows import _COVERAGE
from pipelines.datasets.us_fec_campaign_finance.utils import (
    architecture_columns,
)


def _spec_columns(spec) -> list[str]:
    """Column names a coverage spec will query, whatever its date_column shape."""
    dc = spec.date_column
    return [
        getattr(dc, attr)
        for attr in ("col", "year", "month", "quarter", "day")
        if getattr(dc, attr, None) is not None
    ]


@pytest.mark.parametrize("table", sorted(_COVERAGE))
def test_coverage_column_exists_in_architecture(table):
    columns = set(architecture_columns(table))
    for name in _spec_columns(_COVERAGE[table]):
        assert name in columns, (
            f"{table}: coverage spec references column {name!r}, which is not in "
            f"the architecture. register_table_materialization_task would fail at "
            f"runtime with 'Unrecognized name: {name}'."
        )


def test_every_refreshed_table_has_a_coverage_spec():
    """The flow indexes `_COVERAGE[table]`, so a missing spec is a KeyError mid-run."""
    missing = set(constants.ALL_TABLES.value) - set(_COVERAGE)
    assert not missing, (
        f"tables refreshed with no coverage spec: {sorted(missing)}"
    )


def test_no_coverage_spec_for_tables_the_flow_never_refreshes():
    """A spec for an unrefreshed table is dead config and usually a typo."""
    extra = set(_COVERAGE) - set(constants.ALL_TABLES.value)
    assert not extra, (
        f"coverage specs for tables not in ALL_TABLES: {sorted(extra)}"
    )
