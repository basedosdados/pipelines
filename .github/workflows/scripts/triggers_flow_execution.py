from argparse import ArgumentParser
from time import sleep

from backend import Backend
from table_test import (
    get_flow_run_state,
    get_materialization_flow_id,
)

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
        "--dataset-id",
        type=str,
        required=True,
        help="Table id",
    )
    arg_parser.add_argument(
        "--table-id",
        type=str,
        required=False,
        default="",
        help="Table id",
    )

    arg_parser.add_argument(
        "--alias",
        type=str,
        required=False,
        default="False",
        help="DBT alias",
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
        default="prod",
        help="Materialization Target.",
    )

    # Add materialization label argument
    arg_parser.add_argument(
        "--materialization-label",
        type=str,
        required=False,
        default="basedosdados",
        help="Materialization label.",
    )

    # Add dbt command label argument
    arg_parser.add_argument(
        "--dbt-command",
        type=str,
        required=False,
        default="run",
        help="Materialization label.",
    )

    # Get arguments
    args = arg_parser.parse_args()
    # Expand `__all__` tables
    backend = Backend(args.graphql_url)

    # Launch materialization flows
    backend = Backend(args.prefect_backend_url)
    flow_id = get_materialization_flow_id(backend, args.prefect_backend_token)
    launched_flow_run_ids = []
    print(
        f"Launching materialization flow for {args.dataset_id}.{args.table_id}"
    )
    parameters = {
        "dataset_id": args.dataset_id,
        "dbt_alias": args.alias,
        "target": args.materialization_target,
        "table_id": args.table_id,
        "dbt_command": args.dbt_command,
        "disable_elementary": False,
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
