"""
Schedules for br_tse_eleicoes (Prefect 3).

``deploy_flows.py`` reads ``flow.deploy_schedules`` (a list of
``{"cron": ..., "timezone": ...}`` dicts, converted to ``Cron`` objects at
deploy time) and skips schedules entirely in the dev pool.

Only the ``br_tse_eleicoes__refresh`` orchestrator is scheduled; the individual
table flows have no cron and are triggered by the orchestrator via
``run_deployment``. The TSE publishes consolidated election data with a long
lag, so a weekly check during/after an election year is enough.
"""

from __future__ import annotations

# Empty by default — enable in an election year by pointing the orchestrator's
# deploy_schedules at REFRESH_SCHEDULES (see flows.py).
REFRESH_SCHEDULES: list[dict] = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"},  # Mon 06:00 BRT
]
