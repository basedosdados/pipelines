# -*- coding: utf-8 -*-
import pandas as pd
from prefect import Client


def delete_flow_run(client: Client, flow: dict) -> bool:
    mutation = """
        mutation {
        delete_flow_run(input: {flow_run_id: "%s"}) {
            success
        }
        }
        """ % flow["id"]

    return client.graphql(query=mutation)["data"]["delete_flow_run"]


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

    df = pd.DataFrame.from_records(r["data"]["flow"])
    archieved_flow_runs = df["flow_runs"].str.len() != 0


    for _,flow in df[archieved_flow_runs].iterrows():
        print(f'deleting {len(flow["flow_runs"])} archieved runs for flow {flow["name"]}\n')
        for scheduled_run in flow["flow_runs"]:
            delete_flow_run(client, scheduled_run)


if __name__ == "__main__":
    delete_archieved_flow_runs()
