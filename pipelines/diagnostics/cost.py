"""Attribute BigQuery spend to the dataset that caused it.

The recurring pipelines share one daily processing quota, and when it trips the
failure surfaces on whichever pipeline happens to run next — not on the one
responsible. `INFORMATION_SCHEMA.JOBS_BY_PROJECT` carries the bytes billed per
job and the referenced tables per job, which is enough to rank datasets by the
spend they actually cause.

Pure query construction and row folding live here so they can be unit-tested;
execution needs BigQuery credentials for the billing project.
"""

from __future__ import annotations

import dataclasses

# `referenced_tables` is repeated, so the UNNEST fans a job out across every
# table it touched. Bytes are attributed to each referenced dataset rather than
# split between them: a job scanning two datasets genuinely costs both of them
# that scan, and splitting would understate a shared model's true cost.
JOBS_QUERY = """
select
    ref.dataset_id as dataset_id,
    count(distinct j.job_id) as jobs,
    sum(j.total_bytes_billed) as bytes_billed,
    sum(
        case when j.statement_type = 'SELECT' then j.total_bytes_billed else 0 end
    ) as bytes_billed_select,
    countif(j.error_result is not null) as failed_jobs
from `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT as j,
    unnest(j.referenced_tables) as ref
where
    j.creation_time >= timestamp_sub(current_timestamp(), interval {days} day)
    and j.job_type = 'QUERY'
    and j.total_bytes_billed is not null
group by dataset_id
order by bytes_billed desc
"""

TIB = 1024**4


@dataclasses.dataclass(frozen=True)
class DatasetCost:
    """One dataset's BigQuery spend over the window."""

    dataset_id: str
    jobs: int
    bytes_billed: int
    bytes_billed_select: int
    failed_jobs: int

    @property
    def tib_billed(self) -> float:
        return self.bytes_billed / TIB

    @property
    def usd_estimate(self) -> float:
        """On-demand analysis pricing, US multi-region, $6.25/TiB (2026-08)."""
        return self.tib_billed * 6.25


def build_query(project: str, days: int, region: str = "us") -> str:
    """Render the jobs query.

    Args:
        project: Billing project whose jobs are read (e.g. ``"basedosdados"``).
        days: Trailing window in days.
        region: BigQuery region qualifier, without the ``region-`` prefix.

    Returns:
        The SQL to execute.

    Raises:
        ValueError: If `days` is not a positive integer, since it is
            interpolated into the SQL rather than bound as a parameter.
    """
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        raise ValueError(f"days must be a positive int, got {days!r}")
    if not project.replace("-", "").replace("_", "").isalnum():
        raise ValueError(f"suspicious project id: {project!r}")
    if not region.replace("-", "").isalnum():
        raise ValueError(f"suspicious region: {region!r}")
    return JOBS_QUERY.format(project=project, region=region, days=days)


def fold_rows(rows) -> list[DatasetCost]:
    """Turn query rows into `DatasetCost`, heaviest first.

    Args:
        rows: Iterable of mappings with the query's column names.

    Returns:
        Costs sorted by bytes billed, descending.
    """
    out = [
        DatasetCost(
            dataset_id=r["dataset_id"],
            jobs=int(r["jobs"]),
            bytes_billed=int(r["bytes_billed"] or 0),
            bytes_billed_select=int(r["bytes_billed_select"] or 0),
            failed_jobs=int(r["failed_jobs"] or 0),
        )
        for r in rows
    ]
    return sorted(out, key=lambda c: c.bytes_billed, reverse=True)


def format_report(costs: list[DatasetCost], days: int, top: int = 25) -> str:
    """Render a ranked plain-text report.

    Args:
        costs: Folded costs, any order.
        days: The window the costs cover, for the header.
        top: How many rows to print. The omitted tail is summarized, never
            silently dropped.

    Returns:
        The report text.
    """
    costs = sorted(costs, key=lambda c: c.bytes_billed, reverse=True)
    total = sum(c.bytes_billed for c in costs)
    lines = [
        f"BigQuery spend by dataset — trailing {days}d",
        f"{'dataset':<44}{'TiB':>9}{'USD~':>9}{'jobs':>7}{'fail':>6}{'share':>7}",
        "-" * 82,
    ]
    for c in costs[:top]:
        share = (c.bytes_billed / total * 100) if total else 0.0
        lines.append(
            f"{c.dataset_id[:44]:<44}{c.tib_billed:>9.2f}"
            f"{c.usd_estimate:>9.0f}{c.jobs:>7}{c.failed_jobs:>6}{share:>6.1f}%"
        )
    if len(costs) > top:
        tail = costs[top:]
        tail_bytes = sum(c.bytes_billed for c in tail)
        lines.append(
            f"{f'... {len(tail)} more datasets':<44}"
            f"{tail_bytes / TIB:>9.2f}"
            f"{tail_bytes / TIB * 6.25:>9.0f}"
            f"{sum(c.jobs for c in tail):>7}"
            f"{sum(c.failed_jobs for c in tail):>6}"
            f"{(tail_bytes / total * 100) if total else 0:>6.1f}%"
        )
    lines.append("-" * 82)
    lines.append(
        f"{'TOTAL':<44}{total / TIB:>9.2f}{total / TIB * 6.25:>9.0f}"
        f"{sum(c.jobs for c in costs):>7}{sum(c.failed_jobs for c in costs):>6}"
    )
    return "\n".join(lines)


def run(project: str = "basedosdados", days: int = 7, top: int = 25) -> str:
    """Execute the query and render the report. Needs BigQuery credentials.

    Args:
        project: Billing project whose job history is read.
        days: Trailing window in days.
        top: Rows to print.

    Returns:
        The report text.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    rows = client.query(build_query(project, days)).result()
    return format_report(fold_rows(dict(r) for r in rows), days=days, top=top)
