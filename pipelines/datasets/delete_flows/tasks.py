# -*- coding: utf-8 -*-
"""
Tasks for delete_flows
"""
from typing import Dict, List
from prefect import task
from prefect.client import Client
import pendulum
from pipelines.utils.utils import log


@task(checkpoint=False)
def get_prefect_client() -> Client:
    """
    Returns the prefect client
    """
    return Client()


@task
def get_old_flows_runs(days_old: int, client: Client = None) -> List[Dict[str, str]]:
    """
    Fetches old flow runs from the API.
    Args:
        days_old (int): The age of the flow runs (in days) to fetch.
    Returns:
        A list containing one dictionary for every flow we got. The format for the
        dictionary is the following:
    ```py
    {
        "id": "some-uuid4",
        "state": "the-final-state-for-this-flow",
        "start_time": "2022-01-01T00:00:00.000000+00:00
    }
    ```
    """
    maximum_start_time = (
        pendulum.now(tz="America/Sao_Paulo").subtract(days=days_old).to_iso8601_string()
    )
    if not client:
        client = Client()
    query = """
        query($maximum_start_time: timestamptz) {
            flow_run (
                where: {
                    _and: [
                        {start_time: {_lte: $maximum_start_time}},
                    ]
                }
            ) {
                id
                state
                start_time
            }
        }
    """
    response = client.graphql(
        query=query, variables=dict(maximum_start_time=maximum_start_time)
    )
    return response["data"]["flow_run"]


@task
def delete_flow_run(flow_run_dict: Dict[str, str], client: Client = None) -> None:
    """
    Deletes a flow run from the API.
    """
    flow_run_id = flow_run_dict["id"]
    log(f">>>>>>>>>> Deleting flow run {flow_run_id}")
    if not client:
        client = Client()
    query = """
        mutation($flow_run_id: UUID!) {
            delete_flow_run (
                input: {
                    flow_run_id: $flow_run_id
                }
            ) {
                success
            }
        }
    """
    response = client.graphql(query=query, variables=dict(flow_run_id=flow_run_id))
    success: bool = response["data"]["delete_flow_run"]["success"]
    log(type(response["data"]["delete_flow_run"]))
    log(response["data"])
    if not success:
        # TODO: add exception only if it's not a 404. Need to wait for a failure too see what the response is like.
