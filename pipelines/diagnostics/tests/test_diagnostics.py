"""Tests for the pure halves of the pipeline diagnostics."""

from __future__ import annotations

import pytest

from pipelines.diagnostics.cost import (
    DatasetCost,
    build_query,
    fold_rows,
    format_report,
)
from pipelines.diagnostics.health import (
    Outcome,
    RunOutcome,
    classify_run,
    summarize,
)

NO_UPDATE = (
    "Não há novas atualizações na fonte original — fonte em 2026-06-01, "
    "coverage em 2026-06-01"
)
HAS_UPDATE = (
    "Há atualizações na fonte original — fonte em 2026-07-01, "
    "coverage em 2026-06-01"
)


# --------------------------------------------------------------------- health
def test_completed_poll_noop_is_not_an_ingest():
    """The bug this exists to catch: green, but nothing moved."""
    assert (
        classify_run("Completed", ["Beginning flow run", NO_UPDATE])
        is Outcome.POLLED_NO_NEW_DATA
    )


def test_completed_with_dbt_build_is_an_ingest():
    assert (
        classify_run("Completed", [HAS_UPDATE, "dbt run OK: models/x.sql"])
        is Outcome.INGESTED
    )


def test_completed_without_any_marker_is_flagged_not_guessed():
    assert (
        classify_run("Completed", ["Beginning flow run"])
        is Outcome.COMPLETED_WITHOUT_SIGNAL
    )


@pytest.mark.parametrize("state", ["Failed", "Crashed", "Cancelled"])
def test_non_completed_states_are_failures(state):
    assert classify_run(state, []) is Outcome.FAILED


def test_flow_that_never_ingested_is_suspicious():
    runs = [
        RunOutcome(
            "br_ibge_ipca", str(i), "Completed", Outcome.POLLED_NO_NEW_DATA
        )
        for i in range(60)
    ]

    (health,) = summarize(runs)

    assert health.total == 60
    assert health.ingested == 0
    assert health.ingest_rate == 0.0
    assert health.is_suspicious


def test_a_few_runs_without_ingest_is_not_yet_suspicious():
    """A monthly source polled daily legitimately shows long quiet stretches."""
    runs = [
        RunOutcome("x", str(i), "Completed", Outcome.POLLED_NO_NEW_DATA)
        for i in range(4)
    ]

    assert not summarize(runs)[0].is_suspicious


def test_summarize_puts_suspicious_flows_first():
    runs = [
        RunOutcome("healthy", "1", "Completed", Outcome.INGESTED),
        *[
            RunOutcome("dead", str(i), "Completed", Outcome.POLLED_NO_NEW_DATA)
            for i in range(6)
        ],
    ]

    assert [h.flow_name for h in summarize(runs)] == ["dead", "healthy"]


# ----------------------------------------------------------------------- cost
def test_build_query_rejects_non_positive_days():
    """`days` is interpolated, not bound — so it must be validated."""
    for bad in (0, -1, "7", True):
        with pytest.raises(ValueError):
            build_query("basedosdados", bad)


def test_build_query_rejects_suspicious_identifiers():
    with pytest.raises(ValueError):
        build_query("proj`; drop table x--", 7)
    with pytest.raises(ValueError):
        build_query("basedosdados", 7, region="us`--")


def test_build_query_embeds_project_region_and_window():
    sql = build_query("basedosdados", 7)

    assert (
        "`basedosdados`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql
    )
    assert "interval 7 day" in sql


def test_fold_rows_sorts_by_bytes_and_handles_nulls():
    costs = fold_rows(
        [
            {
                "dataset_id": "small",
                "jobs": 1,
                "bytes_billed": 10,
                "bytes_billed_select": None,
                "failed_jobs": None,
            },
            {
                "dataset_id": "big",
                "jobs": 2,
                "bytes_billed": 1000,
                "bytes_billed_select": 500,
                "failed_jobs": 1,
            },
        ]
    )

    assert [c.dataset_id for c in costs] == ["big", "small"]
    assert costs[1].bytes_billed_select == 0
    assert costs[1].failed_jobs == 0


def test_format_report_summarizes_the_tail_rather_than_dropping_it():
    costs = [
        DatasetCost(
            f"ds_{i}",
            jobs=1,
            bytes_billed=(100 - i),
            bytes_billed_select=0,
            failed_jobs=0,
        )
        for i in range(30)
    ]

    report = format_report(costs, days=7, top=5)

    assert "... 25 more datasets" in report
    assert "TOTAL" in report


def test_tib_and_usd_conversion():
    cost = DatasetCost(
        "x", jobs=1, bytes_billed=1024**4, bytes_billed_select=0, failed_jobs=0
    )

    assert cost.tib_billed == 1.0
    assert cost.usd_estimate == pytest.approx(6.25)
