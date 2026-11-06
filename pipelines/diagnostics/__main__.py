"""CLI for the pipeline diagnostics.

    uv run python -m pipelines.diagnostics cost --days 7
    uv run python -m pipelines.diagnostics health --days 30

`cost` reads INFORMATION_SCHEMA.JOBS_BY_PROJECT, which is itself free to query
but needs `bigquery.jobs.listAll` on the project — `roles/bigquery.resourceViewer`
grants it. A service account with only dataset-level access gets a 403 here; that
is a permissions gap, not a broken query.

`health` needs a Prefect connection (PREFECT_API_URL / PREFECT_API_KEY, as the
workers use).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC


def _cost(args: argparse.Namespace) -> str:
    from pipelines.diagnostics.cost import run

    return run(project=args.project, days=args.days, top=args.top)


async def _collect_health(days: int, limit: int):
    """Pull recent runs and their logs from Prefect, classified.

    Kept out of `health.py` so the classification stays importable without a
    Prefect connection.
    """
    from datetime import datetime, timedelta

    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        FlowRunFilter,
        FlowRunFilterStartTime,
        LogFilter,
        LogFilterFlowRunId,
    )

    from pipelines.diagnostics.health import RunOutcome, classify_run

    since = datetime.now(UTC) - timedelta(days=days)
    outcomes = []
    async with get_client() as client:
        runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                start_time=FlowRunFilterStartTime(after_=since)
            ),
            limit=limit,
        )
        for run in runs:
            logs = await client.read_logs(
                log_filter=LogFilter(
                    flow_run_id=LogFilterFlowRunId(any_=[run.id])
                )
            )
            flow = await client.read_flow(run.flow_id)
            outcomes.append(
                RunOutcome(
                    flow_name=flow.name,
                    run_id=str(run.id),
                    state=run.state_name or "Unknown",
                    outcome=classify_run(
                        run.state_name or "Unknown",
                        [entry.message for entry in logs],
                    ),
                )
            )
    return outcomes


def _health(args: argparse.Namespace) -> str:
    from pipelines.diagnostics.health import format_report, summarize

    outcomes = asyncio.run(_collect_health(args.days, args.limit))
    return format_report(summarize(outcomes), days=args.days)


def main() -> int:
    parser = argparse.ArgumentParser(prog="pipelines.diagnostics")
    sub = parser.add_subparsers(dest="command", required=True)

    cost = sub.add_parser("cost", help="BigQuery spend by dataset")
    cost.add_argument("--project", default="basedosdados")
    cost.add_argument("--days", type=int, default=7)
    cost.add_argument("--top", type=int, default=25)
    cost.set_defaults(fn=_cost)

    health = sub.add_parser("health", help="ingest rate per flow")
    health.add_argument("--days", type=int, default=30)
    health.add_argument("--limit", type=int, default=500)
    health.set_defaults(fn=_health)

    args = parser.parse_args()
    print(args.fn(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
