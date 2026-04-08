import prefect
from prefect import Client

# Increase prefect client request timeout to 60 seconds. Default is 15
prefect.context.config.cloud.request_timeout = 60


def delete_flow_run(client: Client, flow: dict[str, str]) -> bool:
    mutation = """
        mutation($flow_id: UUID!) {
        delete_flow_run(input: {flow_run_id: $flow_id}) {
            success
        }
        }
        """

    return client.graphql(query=mutation, variables={"flow_id": flow["id"]})[
        "data"
    ]["delete_flow_run"]["success"]


def get_archived_flows_with_active_schedule(
    client: Client,
) -> list[dict[str, str]]:
    """Queries all archived flows that still have an active schedule.

    Args:
        client: Authenticated Prefect API client.

    Returns:
        List of flows, each containing ``id`` and ``name``.
    """
    query = """{
        flow(where: {archived: {_eq: true}, is_schedule_active: {_eq: true}}){
        id
        name
        }
    }"""

    print("getting archived flows with active schedules...\n")
    r = client.graphql(query=query)
    flows = r["data"]["flow"]

    if len(flows) == 0:
        print("Not found archived flows with active schedules")

    return flows


def deactivate_schedule(client: Client, flow: dict[str, str]) -> bool:
    """Deactivates the schedule of a single flow via GraphQL mutation.

    Args:
        client: Authenticated Prefect API client.
        flow: Flow dict containing at least ``id``.

    Returns:
        ``True`` if the mutation succeeded, ``False`` otherwise.
    """
    mutation = """
        mutation($flow_id: UUID!) {
        set_schedule_inactive(input: {flow_id: $flow_id}) {
            success
        }
        }
        """

    return client.graphql(query=mutation, variables={"flow_id": flow["id"]})[
        "data"
    ]["set_schedule_inactive"]["success"]


def deactivate_schedules(client: Client, flows: list[dict[str, str]]) -> None:
    """Deactivates the schedule for each flow in the list.

    Args:
        client: Authenticated Prefect API client.
        flows: List of flow dicts, each containing ``id`` and ``name``.

    Raises:
        Exception: If the mutation fails for any flow in the list.
    """
    for flow in flows:
        print(
            f"Deactivating schedule for archived flow {flow['name']}: {flow['id']}"
        )
        deactivated = deactivate_schedule(client, flow)
        if not deactivated:
            msg = f"Failed to deactivate schedule for archived flow: {flow['id']} ({flow['name']})"
            raise Exception(msg)


def deactivate_schedules_from_archived_flows() -> None:
    """Fetches archived flows with active schedules and deactivates them."""
    client = Client()
    flows = get_archived_flows_with_active_schedule(client)
    deactivate_schedules(client, flows)


def delete_archieved_flow_runs():
    client = Client()

    get_scheduled_flow_runs_from_archived_flows = """{
        flow(where: {archived: {_eq: true}}){
        id
        name
        flow_runs(where: {state: {_eq: "Scheduled"}}){
            state
            id
            scheduled_start_time
        }
        }
    }"""

    print("getting schedules flow runs from archived flows...\n")
    r = client.graphql(query=get_scheduled_flow_runs_from_archived_flows)

    flows_to_delete = [
        flow for flow in r["data"]["flow"] if len(flow["flow_runs"]) > 0
    ]

    if len(flows_to_delete) == 0:
        print("Not found archieved flow run to delete")

    for flow in flows_to_delete:
        flow_runs = len(flow["flow_runs"])
        print(
            f"Deleting {flow_runs} archieved flow for {flow['name']}: {flow['id']}"
        )
        for flow_run in flow["flow_runs"]:
            deleted = delete_flow_run(client, flow_run)
            if not deleted:
                msg = f"Failed to delete archived flow run: {flow_run['id']} for flow {flow['name']}"
                raise Exception(msg)


if __name__ == "__main__":
    deactivate_schedules_from_archived_flows()
    delete_archieved_flow_runs()
