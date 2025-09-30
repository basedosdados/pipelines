from prefect import Client


def delete_flow_run(client: Client, flow: dict[str, str]) -> bool:
    mutation = """
        mutation($flow_id: UUID) {
        delete_flow_run(input: {flow_run_id: flow_id}) {
            success
        }
        }
        """

    return client.graphql(query=mutation)["data"]["delete_flow_run"]["success"]


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
    delete_archieved_flow_runs()
