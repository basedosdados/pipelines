# -*- coding: utf-8 -*-
"""
Tasks for br_bd_metadados
"""


import os
from datetime import timedelta
import datetime

import pandas as pd
from pipelines.datasets.br_bd_metadados.utils import extract_and_process_schedule_data, get_skipped_upload_to_gcs_column, save_files_per_week
from prefect import Client, task

from pipelines.constants import constants
from pipelines.utils.utils import log
#register_flow

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_flow_runs(table_id: str = "prefect_flow_runs"):


    graphql_client = Client()

    graphql_query = """{flow_run(where: {_and: [{state: {_nin: ["Scheduled", "Cancelled"]}}]}) {
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

    graphql_response = graphql_client.graphql(query=graphql_query)

    flow_runs_df = pd.json_normalize(graphql_response["data"]["flow_run"], sep="_")

    flow_runs_df["skipped_upload_to_gcs"] = get_skipped_upload_to_gcs_column(flow_runs_df)
    flow_runs_df["dataset_id"] = flow_runs_df["parameters_dataset_id"]
    flow_runs_df["table_id"] = flow_runs_df["parameters_table_id"]
    flow_runs_df['labels'] = flow_runs_df['labels'].str[0]

    relevant_columns = [col for col in flow_runs_df.columns if not col.startswith("parameters")]

    folder_path = f"tmp/{table_id}"
    os.system(f"mkdir -p {folder_path}")

    save_files_per_week(flow_runs_df, relevant_columns,folder_path)

    return folder_path

@task
def crawler_flows(table_id: str = "prefect_flows"):



    graphql_client = Client()

    graphql_query = """{flow(where: {archived: {_eq: false}}) {
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
    }}
    """

    graphql_response = graphql_client.graphql(query=graphql_query)

    flow_df = pd.json_normalize(graphql_response, record_path=["data", "flow"], sep="_")

    flow_df = extract_and_process_schedule_data(flow_df)

    # Define the folder path for storing the file
    output_folder = f"tmp/{table_id}/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {output_folder}")
    # Define the full file path for the CSV file
    csv_file_path = f"{output_folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    flow_df.to_csv(csv_file_path, index=False)

    return csv_file_path
