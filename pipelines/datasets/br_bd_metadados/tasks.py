# -*- coding: utf-8 -*-
"""
Tasks for br_bd_metadados
"""
from datetime import datetime, timedelta

import pandas as pd
from pipelines.datasets.br_bd_metadados.utils import save_input
from prefect import task
from prefect import Client


from pipelines.constants import constants
from pipelines.utils.utils import log


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

    df = pd.json_normalize(r['data']['flow_run'])

    create_table_and_upload_to_gcs_skipped =   {
        'state': 'Skipped',
        'task': {
        'name': 'create_table_and_upload_to_gcs'
        }
    }

    df['skipped_upload_to_gcs'] = df['task_runs'].apply(lambda tasks: create_table_and_upload_to_gcs_skipped in tasks )

    df['dataset_id'] = df['parameters.dataset_id']
    df['table_id'] = df['parameters.table_id']

    filtered_columns = [col for col in df.columns if not col.startswith('parameters')]

    df['labels'] = df['labels'].str[0]

    selected_df = df[filtered_columns]

    return save_input(selected_df,'prefect_flow_runs')
