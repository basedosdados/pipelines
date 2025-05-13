import re
from argparse import ArgumentParser
from time import sleep

from backend import Backend
from utils import expand_alls, get_datasets_tables_from_modified_files


def get_flow_run_state(flow_run_id: str, backend: Backend, auth_token: str):
    query = """
    query ($flow_run_id: uuid!) {
        flow_run_by_pk (id: $flow_run_id) {
            state
        }
    }
    """
    response = backend._execute_query(
        query,
        variables={"flow_run_id": flow_run_id},
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    return response["flow_run_by_pk"]["state"]


def get_flow_status_logs(flow_run_id: str, backend: Backend, auth_token: str):
    query = """query ($flow_run_id: uuid!){
                    log(where:{
                            flow_run_id:{_eq:$flow_run_id},
                            message:{_like:"%Done.%"}}){
                            message
                    }
                    }"""
    response = backend._execute_query(
        query,
        variables={"flow_run_id": flow_run_id},
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    print(response)
    message = response["log"]["message"]
    result = {}
    result["pass"] = int(re.findall("PASS=\d+", message)[0].split("=")[1])
    result["skip"] = int(re.findall("SKIP=\d+", message)[0].split("=")[1])
    result["warn"] = int(re.findall("WARN=\d+", message)[0].split("=")[1])

    return result


def get_materialization_flow_id(
    backend: Backend, auth_token: str, project: str = "main"
):
    query = """
    query ($projectName: String!) {
        flow (where: {
            name: {
                _like: "BD template: Executa DBT model"
            },
            archived: {
                _eq: false
            },
            project: {
                name: {_eq: $projectName}
            }
        }) {
            id
        }
    }
    """
    response = backend._execute_query(
        query,
        headers={"Authorization": f"Bearer {auth_token}"},
        variables={"projectName": project},
    )
    return response["flow"][0]["id"]


if __name__ == "__main__":
    # Start argument parser
    arg_parser = ArgumentParser()

    # Add GraphQL URL argument
    arg_parser.add_argument(
        "--graphql-url",
        type=str,
        required=True,
        help="URL of the GraphQL endpoint.",
    )

    # Add list of modified files argument
    arg_parser.add_argument(
        "--modified-files",
        type=str,
        required=True,
        help="List of modified files.",
    )

    # Add Prefect backend URL argument
    arg_parser.add_argument(
        "--prefect-backend-url",
        type=str,
        required=False,
        default="https://prefect.basedosdados.org/api",
        help="Prefect backend URL.",
    )

    # Add prefect base URL argument
    arg_parser.add_argument(
        "--prefect-base-url",
        type=str,
        required=False,
        default="https://prefect.basedosdados.org",
        help="Prefect base URL.",
    )

    # Add Prefect API token argument
    arg_parser.add_argument(
        "--prefect-backend-token",
        type=str,
        required=True,
        help="Prefect backend token.",
    )

    # Add materialization mode argument
    arg_parser.add_argument(
        "--materialization-target",
        type=str,
        required=False,
        default="dev",
        help="Materialization target.",
    )

    # Add materialization label argument
    arg_parser.add_argument(
        "--materialization-label",
        type=str,
        required=False,
        default="basedosdados-dev",
        help="Materialization label.",
    )

    # Add dbt command label argument
    arg_parser.add_argument(
        "--dbt-command",
        type=str,
        required=False,
        default="test",
        help="Materialization label.",
    )

    # Get arguments
    args = arg_parser.parse_args()

    # Get datasets and tables from modified files
    modified_files = args.modified_files.split(",")
    datasets_tables = get_datasets_tables_from_modified_files(
        modified_files, show_details=True
    )
    # Split deleted datasets and tables
    deleted_datasets_tables = []
    existing_datasets_tables = []
    for dataset_id, table_id, exists, alias in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id, alias))
        else:
            deleted_datasets_tables.append((dataset_id, table_id, alias))
    # Expand `__all__` tables
    backend = Backend(args.graphql_url)
    expanded_existing_datasets_tables = []
    for dataset_id, table_id, alias in existing_datasets_tables:
        expanded_table_ids = expand_alls(dataset_id, table_id, backend)
        for expanded_dataset_id, expanded_table_id in expanded_table_ids:
            expanded_existing_datasets_tables.append(
                (expanded_dataset_id, expanded_table_id, alias)
            )
    existing_datasets_tables = expanded_existing_datasets_tables

    # Launch materialization flows
    backend = Backend(args.prefect_backend_url)
    flow_id = get_materialization_flow_id(
        backend, args.prefect_backend_token, "staging"
    )
    launched_flow_run_ids = []
    for dataset_id, table_id, alias in existing_datasets_tables:
        print(
            f"Launching flow for testing {dataset_id}.{table_id} (alias={alias})..."
        )
        parameters = {
            "dataset_id": dataset_id,
            "dbt_alias": alias,
            "target": args.materialization_target,
            "table_id": table_id,
            "dbt_command": args.dbt_command,
            "download_csv_file": False,
        }

        mutation = """
        mutation ($flow_id: UUID, $parameters: JSON, $label: String!) {
            create_flow_run (input: {
                flow_id: $flow_id,
                parameters: $parameters,
                labels: [$label],
            }) {
                id
            }
        }
        """
        variables = {
            "flow_id": flow_id,
            "parameters": parameters,
            "label": args.materialization_label,
        }

        response = backend._execute_query(
            mutation,
            variables,
            headers={"Authorization": f"Bearer {args.prefect_backend_token}"},
        )

        flow_run_id = response["create_flow_run"]["id"]
        launched_flow_run_ids.append(flow_run_id)
        flow_run_url = f"{args.prefect_base_url}/flow-run/{flow_run_id}"
        print(f" - Materialization flow run launched: {flow_run_url}")

    # Keep monitoring the launched flow runs until they are finished
    for launched_flow_run_id in launched_flow_run_ids:
        print(f"Monitoring flow run {launched_flow_run_id}...")
        flow_run_state = get_flow_run_state(
            flow_run_id=launched_flow_run_id,
            backend=backend,
            auth_token=args.prefect_backend_token,
        )
        while flow_run_state not in ["Success", "Failed", "Cancelled"]:
            sleep(5)
            flow_run_state = get_flow_run_state(
                flow_run_id=launched_flow_run_id,
                backend=backend,
                auth_token=args.prefect_backend_token,
            )
        if flow_run_state != "Success":
            raise Exception(
                f'Flow run {launched_flow_run_id} finished with state "{flow_run_state}". '
                f"Check the logs at {args.prefect_base_url}/flow-run/{launched_flow_run_id}"
            )
        else:
            print("Congrats! Everything seems fine!")
