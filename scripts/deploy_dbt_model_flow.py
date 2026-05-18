"""Deploy `run_dbt_model_flow` to the Prefect 3 `basedosdados-dev` work pool.

Usage:
    PREFECT_API_URL=https://prefect3.basedosdados.org/api \
    PREFECT_API_KEY=<key> \
    uvx --from "prefect==3.*" python scripts/deploy_dbt_model_flow.py [branch]
"""

from __future__ import annotations

import sys

from prefect import flow
from prefect.runner.storage import GitRepository

REPO_URL = "https://github.com/basedosdados/pipelines.git"
ENTRYPOINT = "pipelines/utils/execute_dbt_model/flows.py:run_dbt_model_flow"


def main() -> None:
    branch = sys.argv[1] if len(sys.argv) > 1 else "feat/prefect3"

    deployment_id = flow.from_source(
        source=GitRepository(url=REPO_URL, branch=branch),
        entrypoint=ENTRYPOINT,
    ).deploy(
        name="bd-template-executa-dbt-model",
        work_pool_name="basedosdados-dev",
        tags=["dbt", "template", "basedosdados-dev"],
        description="Template flow para executar modelos dbt (run/test).",
    )
    print(f"Deployed: {deployment_id}")


if __name__ == "__main__":
    main()
