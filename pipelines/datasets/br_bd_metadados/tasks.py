# -*- coding: utf-8 -*-
"""
Tasks for br_bd_metadados
"""
from datetime import datetime, timedelta
import os 
import pandas as pd

from prefect import task
from prefect import Client

from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler():
    client = Client()

    body = """{
    flow_run(where: {_and: [{state: {_nin: ["Scheduled", "Cancelled"]}}]}) {
        id
        name
        start_time
        end_time
        labels
        flow {
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

    r = client.graphql(
                    query= body
                )

    df = pd.json_normalize(r['data']['flow_run'],sep='_')

    create_table_and_upload_to_gcs_skipped =   {
        'state': 'Skipped',
        'task': {
        'name': 'create_table_and_upload_to_gcs'
        }
    }

    df['skipped_upload_to_gcs'] = df['task_runs'].apply(lambda tasks: create_table_and_upload_to_gcs_skipped in tasks )

    df['dataset_id'] = df['parameters_dataset_id']
    df['table_id'] = df['parameters_table_id']

    filtered_columns = [col for col in df.columns if not col.startswith('parameters')]

    df['labels'] = df['labels'].str[0]

    selected_df = df[filtered_columns]
    
    table_id = 'prefect_flow_runs'

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    selected_df.to_csv(full_filepath, index=False)

    return full_filepath
