# -*- coding: utf-8 -*-
import pandas as pd
from pipelines.utils.utils import log
import datetime

### prefect flow_runs

def get_skipped_upload_to_gcs_column(flow_runs_df):
    skipped_upload_filter = {
            "state": "Skipped",
            "task": {"name": "create_table_and_upload_to_gcs"},
        }

    flow_runs_df["skipped_upload_to_gcs"] = flow_runs_df["task_runs"].apply(
        lambda tasks: skipped_upload_filter in tasks
    )
    return flow_runs_df["skipped_upload_to_gcs"]


def save_files_per_week(flow_runs_df, relevant_columns,folder_path):

    flow_runs_df['week_start'] = flow_runs_df['end_time'].str[:-6].astype("datetime64[ns]").dt.to_period('W').dt.start_time
    unique_weeks = flow_runs_df['week_start'].unique()


    for week_start in unique_weeks:
        if (datetime.datetime.now() - week_start).days < 14:
            str_week_start = week_start.strftime('%Y%m%d')
            csv_file_path = f"{folder_path}/{str_week_start}.csv"

            week_data_filter = flow_runs_df['week_start'] == week_start
            relevant_df = flow_runs_df[relevant_columns]

            relevant_df[relevant_columns][week_data_filter].to_csv(csv_file_path, index=False)
            log(f"Arquivo da semana {str_week_start} salvo")


### prefect_flows

def extract_and_process_schedule_data(flow_df):
    is_scheduled_filter = flow_df["schedule_clocks"].notna()
    scheduled_flows_df = flow_df[is_scheduled_filter].copy()
    scheduled_flows_df = parse_schedule_information(scheduled_flows_df.reset_index())

    return pd.concat([flow_df[~is_scheduled_filter], scheduled_flows_df], axis=0 )

def parse_schedule_information(flows_df):
    flows_df = flows_df.reset_index(drop=True)
    clocks_df = parse_schedule_clocks_column(flows_df)
    parameters_df = parse_schedule_parameter_column(clocks_df)

    flows_df.drop(columns=["schedule_clocks"], inplace=True)

    return  pd.concat([parameters_df, clocks_df,flows_df], axis=1)

def parse_schedule_clocks_column(flows_df):
    clocks_df = pd.json_normalize(
        flows_df["schedule_clocks"].str[0], max_level=0, sep="_"
    ).add_prefix("schedule_")

    clocks_df.drop(columns=["schedule___version__"], inplace=True)

    return clocks_df

def parse_schedule_parameter_column(clocks_df):
    parameters_df = pd.json_normalize(clocks_df["schedule_parameter_defaults"]).add_prefix("schedule_parameters_")

    standard_params = [
        "schedule_parameters_table_id",
        "schedule_parameters_dbt_alias",
        "schedule_parameters_dataset_id",
        "schedule_parameters_update_metadata",
        "schedule_parameters_materialization_mode",
        "schedule_parameters_materialize_after_dump",
    ]

    return parameters_df[standard_params]