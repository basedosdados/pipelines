"""
Tasks for dumping data directly from BigQuery to GCS.
"""

from basedosdados.core.base import Base
from prefect import task

from pipelines.utils.utils import (
    log,
)


@task
def get_project_id(
    project_id: str | None = None,
    bd_project_mode: str = "prod",
):
    """
    Get the project ID.
    """
    if project_id:
        return project_id
    log(
        "Project ID was not provided, trying to get it from environment variable"
    )
    try:
        bd_base = Base()
        project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
    except KeyError:
        pass
    if not project_id:
        raise ValueError(
            "project_id must be either provided or inferred from environment variables"
        )
    log(f"Project ID was inferred from environment variables: {project_id}")
    return project_id
