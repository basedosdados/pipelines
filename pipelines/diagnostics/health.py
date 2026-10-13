"""Tell a working pipeline apart from one that is green but ingesting nothing.

A scheduled run whose source poll finds nothing new returns early and Prefect
records `COMPLETED`. By state alone a dead pipeline is indistinguishable from a
healthy one — `br_ibge_ipca` sat at 4 ingests across 60 completed runs before
anyone noticed. The only reliable signal is in the logs: the poll logs one of
two lines depending on whether it found new data.

Log classification is pure and unit-tested here; fetching the runs needs a
Prefect connection.
"""

from __future__ import annotations

import dataclasses
import enum

# Both emitted by `pipelines.utils.metadata.register.poll_source_for_update`.
# Matched as substrings so the trailing "— fonte em X, coverage em Y" detail,
# which varies per run, does not affect classification.
_NO_UPDATE_MARKERS = ("Não há novas atualizações na fonte original",)
_HAS_UPDATE_MARKERS = ("Há atualizações na fonte original",)
# Emitted by `run_dbt` once per successful model build against prod.
_MATERIALIZED_MARKERS = ("dbt run OK",)


class Outcome(enum.Enum):
    """What a completed run actually did."""

    INGESTED = "ingested"
    POLLED_NO_NEW_DATA = "polled_no_new_data"
    COMPLETED_WITHOUT_SIGNAL = "completed_without_signal"
    FAILED = "failed"


@dataclasses.dataclass(frozen=True)
class RunOutcome:
    """One flow run, classified."""

    flow_name: str
    run_id: str
    state: str
    outcome: Outcome


def classify_run(state: str, log_messages: list[str]) -> Outcome:
    """Classify one run from its state and log lines.

    `COMPLETED` is deliberately not treated as success: the poll guard returns
    early and still completes. A completed run that shows neither poll marker
    nor a dbt build is reported as `COMPLETED_WITHOUT_SIGNAL` rather than
    guessed either way — that is the case worth a human look.

    Args:
        state: Prefect state name, e.g. ``"Completed"``.
        log_messages: The run's log messages, any order.

    Returns:
        The classified outcome.
    """
    if state.lower() != "completed":
        return Outcome.FAILED

    blob = "\n".join(log_messages)
    if any(m in blob for m in _MATERIALIZED_MARKERS):
        return Outcome.INGESTED
    if any(m in blob for m in _HAS_UPDATE_MARKERS):
        return Outcome.INGESTED
    if any(m in blob for m in _NO_UPDATE_MARKERS):
        return Outcome.POLLED_NO_NEW_DATA
    return Outcome.COMPLETED_WITHOUT_SIGNAL


@dataclasses.dataclass(frozen=True)
class FlowHealth:
    """Aggregate health of one flow over the window."""

    flow_name: str
    total: int
    ingested: int
    polled_no_new_data: int
    completed_without_signal: int
    failed: int

    @property
    def ingest_rate(self) -> float:
        """Share of runs that actually moved data. 0.0 when nothing ran."""
        return (self.ingested / self.total) if self.total else 0.0

    @property
    def is_suspicious(self) -> bool:
        """True when the flow ran repeatedly and never once ingested.

        A low rate is normal — a daily poll on a monthly source ingests ~1 run in
        30. Never ingesting across a meaningful number of runs is not.
        """
        return self.total >= 5 and self.ingested == 0


def summarize(outcomes: list[RunOutcome]) -> list[FlowHealth]:
    """Fold classified runs into per-flow health, most suspicious first.

    Args:
        outcomes: Classified runs across any number of flows.

    Returns:
        One `FlowHealth` per flow, suspicious flows first, then by ingest rate.
    """
    by_flow: dict[str, list[RunOutcome]] = {}
    for o in outcomes:
        by_flow.setdefault(o.flow_name, []).append(o)

    health = [
        FlowHealth(
            flow_name=flow,
            total=len(runs),
            ingested=sum(r.outcome is Outcome.INGESTED for r in runs),
            polled_no_new_data=sum(
                r.outcome is Outcome.POLLED_NO_NEW_DATA for r in runs
            ),
            completed_without_signal=sum(
                r.outcome is Outcome.COMPLETED_WITHOUT_SIGNAL for r in runs
            ),
            failed=sum(r.outcome is Outcome.FAILED for r in runs),
        )
        for flow, runs in by_flow.items()
    ]
    return sorted(health, key=lambda h: (not h.is_suspicious, h.ingest_rate))


def format_report(health: list[FlowHealth], days: int) -> str:
    """Render the per-flow health table.

    Args:
        health: Folded health, any order.
        days: Window the runs cover, for the header.

    Returns:
        The report text.
    """
    health = sorted(health, key=lambda h: (not h.is_suspicious, h.ingest_rate))
    lines = [
        f"Pipeline ingest health — trailing {days}d",
        "A completed run is not an ingest: the source poll returns early and "
        "still completes.",
        "",
        f"{'flow':<40}{'runs':>6}{'ingest':>8}{'noop':>6}"
        f"{'quiet':>7}{'fail':>6}{'rate':>7}",
        "-" * 80,
    ]
    for h in health:
        flag = "  <-- never ingested" if h.is_suspicious else ""
        lines.append(
            f"{h.flow_name[:40]:<40}{h.total:>6}{h.ingested:>8}"
            f"{h.polled_no_new_data:>6}{h.completed_without_signal:>7}"
            f"{h.failed:>6}{h.ingest_rate * 100:>6.0f}%{flag}"
        )
    suspicious = [h.flow_name for h in health if h.is_suspicious]
    lines.append("-" * 80)
    lines.append(
        f"{len(health)} flow(s); never ingested despite >=5 runs: "
        + (", ".join(suspicious) if suspicious else "none")
    )
    lines.append(
        "'quiet' = completed with neither poll marker nor a dbt build — "
        "inspect those directly."
    )
    return "\n".join(lines)
