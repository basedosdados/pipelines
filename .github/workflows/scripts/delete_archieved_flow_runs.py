import prefect
from prefect import Client

# Increase prefect client request timeout to 60 seconds. Default is 15
REQUEST_TIMEOUT = 60
prefect.context.config.cloud.request_timeout = REQUEST_TIMEOUT

Flow = dict[str, str]


# --- Schedule deactivation ---


def get_archived_flows_with_active_schedule(client: Client) -> list[Flow]:
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
    response = client.graphql(query=query)
    flows = response["data"]["flow"]

    if not flows:
        print("Not found archived flows with active schedules")

    return flows


def deactivate_schedule(client: Client, flow: Flow) -> bool:
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

    response = client.graphql(
        query=mutation, variables={"flow_id": flow["id"]}
    )
    return response["data"]["set_schedule_inactive"]["success"]


def deactivate_schedules(client: Client, flows: list[Flow]) -> None:
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


def deactivate_schedules_from_archived_flows(client: Client) -> None:
    """Fetches archived flows with active schedules and deactivates them.

    Args:
        client: Authenticated Prefect API client.
    """
    flows = get_archived_flows_with_active_schedule(client)
    deactivate_schedules(client, flows)


# --- Flow run deletion ---


def get_archived_flows_with_scheduled_runs(client: Client) -> list[dict]:
    """Queries archived flows that have at least one scheduled flow run.

    Args:
        client: Authenticated Prefect API client.

    Returns:
        List of flows, each containing ``id``, ``name``, and a ``flow_runs``
        list with the scheduled runs.
    """
    query = """{
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

    print("getting scheduled flow runs from archived flows...\n")
    response = client.graphql(query=query)
    flows = [flow for flow in response["data"]["flow"] if flow["flow_runs"]]

    if not flows:
        print("Not found archived flow runs to delete")

    return flows


def delete_flow_run(client: Client, flow_run: Flow) -> bool:
    """Deletes a single flow run via GraphQL mutation.

    Args:
        client: Authenticated Prefect API client.
        flow_run: Flow run dict containing at least ``id``.

    Returns:
        ``True`` if the mutation succeeded, ``False`` otherwise.
    """
    mutation = """
        mutation($flow_id: UUID!) {
        delete_flow_run(input: {flow_run_id: $flow_id}) {
            success
        }
        }
        """

    response = client.graphql(
        query=mutation, variables={"flow_id": flow_run["id"]}
    )
    return response["data"]["delete_flow_run"]["success"]


def delete_flow_runs(client: Client, flows: list[dict]) -> None:
    """Deletes all scheduled flow runs for each flow in the list.

    Args:
        client: Authenticated Prefect API client.
        flows: List of flow dicts, each containing ``id``, ``name``, and
            a ``flow_runs`` list.

    Raises:
        Exception: If the deletion mutation fails for any flow run.
    """
    for flow in flows:
        print(
            f"Deleting {len(flow['flow_runs'])} archived flow run(s) for {flow['name']}: {flow['id']}"
        )
        for flow_run in flow["flow_runs"]:
            deleted = delete_flow_run(client, flow_run)
            if not deleted:
                msg = f"Failed to delete archived flow run: {flow_run['id']} for flow {flow['name']}"
                raise Exception(msg)


def delete_archieved_flow_runs(client: Client) -> None:
    """Fetches archived flows with scheduled runs and deletes those runs.

    Args:
        client: Authenticated Prefect API client.
    """
    flows = get_archived_flows_with_scheduled_runs(client)
    delete_flow_runs(client, flows)


# --- Entry point ---


def main() -> None:
    """Creates a single Prefect client and runs all cleanup routines."""
    client = Client()
    deactivate_schedules_from_archived_flows(client)
    delete_archieved_flow_runs(client)


if __name__ == "__main__":
    main()
