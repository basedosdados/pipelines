# -*- coding: utf-8 -*-
"""
Tasks for br_bd_metadados
"""
import os
from datetime import timedelta

import pandas as pd
from prefect import Client, task

from pipelines.constants import constants

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_flow_runs():
    client = Client()

    body = """{flow_run(where: {_and: [{state: {_nin: ["Scheduled", "Cancelled"]}}]}) {
                id
                name
                start_time
                end_time
                labels
                flow {
                    flow_group_id
                    archived
                    name
                    project{
                        name
                    }
                }
                parameters
                state
                state_message
                task_runs {
                    state
                    task {
                        name
                    }
                }
                logs(where: {level: {_eq: "ERROR"}}) {
                    message
                }
            }
            }"""

    r = client.graphql(query=body)

    df = pd.json_normalize(r["data"]["flow_run"], sep="_")

    create_table_and_upload_to_gcs_skipped = {
        "state": "Skipped",
        "task": {"name": "create_table_and_upload_to_gcs"},
    }

    df["skipped_upload_to_gcs"] = df["task_runs"].apply(
        lambda tasks: create_table_and_upload_to_gcs_skipped in tasks
    )

    df["dataset_id"] = df["parameters_dataset_id"]
    df["table_id"] = df["parameters_table_id"]

    filtered_columns = [col for col in df.columns if not col.startswith("parameters")]

    df["labels"] = df["labels"].str[0]

    selected_df = df[filtered_columns]

    table_id = "prefect_flow_runs"

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    selected_df.to_csv(full_filepath, index=False)

    return full_filepath


@task
def crawler_flows():
    client = Client()

    body = """{
    flow(where: {archived: {_eq: false}, is_schedule_active: {_eq: true}}) {
        name
        version
        created
        schedule
        flow_group_id
        project{
        name
        }
        flow_group {
        flows_aggregate {
            aggregate {
            min {
                created
            }
            }
        }
        }
    }
    }
    """

    r = client.graphql(query=body)

    df = pd.json_normalize(r, record_path=["data", "flow"], sep="_")

    df_schedule_clocks = pd.json_normalize(
        df["schedule_clocks"].str[0], max_level=0, sep="_"
    ).add_prefix("schedule_")
    df.drop(
        columns=[
            "schedule_clocks",
        ],
        inplace=True,
    )
    df_schedule_clocks.drop(columns=["schedule___version__"], inplace=True)

    df_parameters = pd.json_normalize(
        df_schedule_clocks["schedule_parameter_defaults"]
    ).add_prefix("schedule_parameters_")
    standard_params = [
        "schedule_parameters_table_id",
        "schedule_parameters_dbt_alias",
        "schedule_parameters_dataset_id",
        "schedule_parameters_update_metadata",
        "schedule_parameters_materialization_mode",
        "schedule_parameters_materialize_after_dump",
    ]

    df_final = pd.concat(
        [df, df_schedule_clocks, df_parameters[standard_params]], axis=1
    )

    table_id = "prefect_flows"

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    df_final.to_csv(full_filepath, index=False)

    return full_filepath
