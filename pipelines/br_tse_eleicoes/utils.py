"""
General purpose helpers for the br_tse_eleicoes orchestration pipeline.
"""

from __future__ import annotations

from pipelines.br_tse_eleicoes.constants import constants


def flow_name(table_id: str) -> str:
    """Prefect flow name for a table (also the deployment's variable name)."""
    return f"{constants.DATASET_ID.value}__{table_id}"


def deployment_name(table_id: str) -> str:
    """Deployment slug used by ``run_deployment`` (``<flow>/<deployment>``).

    ``deploy_flows.py`` registers each flow under ``name=<variable name>`` in
    the work pool, and Prefect addresses a deployment as ``<flow_name>/<name>``.
    Both halves are ``br_tse_eleicoes__<table_id>`` here.
    """
    name = flow_name(table_id)
    return f"{name}/{name}"
