# -*- coding: utf-8 -*-
"""
Custom script for registering flows.
"""

import argparse
import ast
import glob
import hashlib
import importlib
import json
import os
import runpy
import sys
import traceback
from collections import Counter, defaultdict
from pathlib import Path
from time import sleep
from typing import Any

import box
import prefect
from loguru import logger
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.utilities.graphql import EnumValue, compress, with_args

FlowLike = box.Box | prefect.Flow


def build_and_register(
    client: prefect.Client,
    flows: list[FlowLike],
    project_id: str,
    max_retries: int = 5,
    retry_interval: int = 5,
    schedule: bool = True,
) -> Counter:
    """
    (Adapted from Prefect original code.)

    Build and register all flows.

    Args:
        - client (prefect.Client): the prefect client to use
        - flows (List[FlowLike]): the flows to register
        - project_id (str): the project id in which to register the flows

    Returns:
        - Counter: stats about the number of successful, failed, and skipped flows.
    """
    # Finish preparing flows to ensure a stable hash later
    prepare_flows(flows)

    # Group flows by storage instance.
    storage_to_flows = defaultdict(list)
    for flow in flows:
        storage = flow.storage if isinstance(flow, prefect.Flow) else None
        storage_to_flows[storage].append(flow)
        flow.name = flow.name

    # Register each flow, building storage as needed.
    # Stats on success/fail/skip rates are kept for later display
    stats = Counter(registered=0, errored=0, skipped=0)
    for storage, _flows in storage_to_flows.items():
        # Build storage if needed
        if storage is not None:
            logger.info(f"  Building `{type(storage).__name__}` storage...")
            try:
                storage.build()
            except Exception:  # pylint: disable=broad-except
                logger.error("    Error building storage:")
                logger.error(traceback.format_exc())
                for flow in _flows:
                    logger.error(f"  Registering {flow.name!r}...")
                    stats["errored"] += 1
                continue

        for flow in _flows:
            logger.info(f"  Registering {flow.name!r}...", nl=False)
            try:
                if isinstance(flow, box.Box):
                    serialized_flow = flow
                else:
                    serialized_flow = flow.serialize(build=False)

                attempts = 0
                while attempts < max_retries:
                    attempts += 1
                    try:
                        (
                            flow_id,
                            flow_version,
                            is_new,
                        ) = register_serialized_flow(
                            client=client,
                            serialized_flow=serialized_flow,
                            project_id=project_id,
                            schedule=schedule,
                        )
                        break
                    except Exception:  # pylint: disable=broad-except
                        logger.error("Error registering flow:")
                        logger.error(traceback.format_exc())
                        if attempts < max_retries:
                            logger.error(
                                f"Retrying in {retry_interval} seconds..."
                            )
                            sleep(retry_interval)
                        else:
                            stats["errored"] += 1
                            continue

            except Exception:  # pylint: disable=broad-except
                logger.error(" Error")
                logger.error(traceback.format_exc())
                stats["errored"] += 1
            else:
                if is_new:
                    logger.success(" Done")
                    logger.success(f"  └── ID: {flow_id}")
                    logger.success(f"  └── Version: {flow_version}")
                    stats["registered"] += 1
                else:
                    logger.warning(
                        " Skipped (metadata unchanged)", fg="yellow"
                    )
                    stats["skipped"] += 1
    return stats


def collect_flows(
    paths: list[str],
) -> dict[str, list[FlowLike]]:
    """
    (Adapted from Prefect original code.)

    Load all flows found in `paths` & `modules`.

    Args:
        - paths (List[str]): file paths to load flows from.
    """

    out = {}
    for p in paths:  # pylint: disable=invalid-name
        flows = load_flows_from_script(p)
        out[p] = flows

    # Drop empty sources
    out = {source: flows for source, flows in out.items() if flows}

    return out


def expand_paths(paths: list[str]) -> list[str]:
    """
    (Adapted from Prefect original code.)

    Given a list of paths, expand any directories to find all contained
    python files.
    """
    out = []
    globbed_paths = set()
    for path in tuple(paths):
        found_paths = glob.glob(path, recursive=True)
        if not found_paths:
            raise Exception(f"Path {path!r} doesn't exist")
        globbed_paths.update(found_paths)
    for path in globbed_paths:
        if os.path.isdir(path):
            with os.scandir(path) as directory:
                out.extend(
                    e.path
                    for e in directory
                    if e.is_file() and e.path.endswith(".py")
                )
        else:
            out.append(path)
    return out


def get_project_id(client: prefect.Client, project: str) -> str:
    """
    (Adapted from Prefect original code.)

    Get a project id given a project name.

    Args:
        - project (str): the project name

    Returns:
        - str: the project id
    """
    resp = client.graphql(
        {
            "query": {
                with_args("project", {"where": {"name": {"_eq": project}}}): {
                    "id"
                }
            }
        }
    )
    if resp.data.project:
        return resp.data.project[0].id
    raise Exception(f"Project {project!r} does not exist")


def load_flows_from_script(path: str) -> list[prefect.Flow]:
    """
    (Adapted from Prefect original code.)

    Given a file path, load all flows found in the file
    """
    # We use abs_path for everything but logging (logging the original
    # user-specified path provides a clearer message).
    abs_path = os.path.abspath(path)
    # Temporarily add the flow's local directory to `sys.path` so that local
    # imports work. This ensures that `sys.path` is the same as it would be if
    # the flow script was run directly (i.e. `python path/to/flow.py`).
    orig_sys_path = sys.path.copy()
    sys.path.insert(0, os.path.dirname(abs_path))
    try:
        with prefect.context(
            {"loading_flow": True, "local_script_path": abs_path}
        ):
            namespace = runpy.run_path(abs_path, run_name="<flow>")
    except Exception as exc:
        logger.error(f"Error loading {path!r}:", fg="red")
        logger.error(traceback.format_exc())
        raise Exception from exc
    finally:
        sys.path[:] = orig_sys_path

    flows = [f for f in namespace.values() if isinstance(f, prefect.Flow)]
    if flows:
        for f in flows:  # pylint: disable=invalid-name
            if f.storage is None:
                f.storage = Local(path=abs_path, stored_as_script=True)
    return flows


def prepare_flows(flows: list[FlowLike]) -> None:
    """
    (Adapted from Prefect original code.)

    Finish preparing flows.

    Shared code between `register` and `build` for any flow modifications
    required before building the flow's storage. Modifies the flows in-place.
    """
    labels = ()

    # Finish setting up all flows before building, to ensure a stable hash
    # for flows sharing storage instances
    for flow in flows:
        if isinstance(flow, dict):
            # Add any extra labels to the flow
            if flow.get("environment"):
                new_labels = set(
                    flow["environment"].get("labels") or []
                ).union(labels)
                flow["environment"]["labels"] = sorted(new_labels)
            else:
                new_labels = set(flow["run_config"].get("labels") or []).union(
                    labels
                )
                flow["run_config"]["labels"] = sorted(new_labels)
        else:
            # Set the default flow result if not specified
            if not flow.result:
                flow.result = flow.storage.result

            # Add a `run_config` if not configured explicitly
            if flow.run_config is None and flow.environment is None:
                flow.run_config = UniversalRun()
            # Add any extra labels to the flow (either specified via the CLI,
            # or from the storage object).
            obj = flow.run_config or flow.environment
            obj.labels.update(labels)
            obj.labels.update(flow.storage.labels)

            # Add the flow to storage
            flow.storage.add_flow(flow)


def register_serialized_flow(
    client: prefect.Client,
    serialized_flow: dict,
    project_id: str,
    force: bool = False,
    schedule: bool = True,
) -> tuple[str, int, bool]:
    """
    (Adapted from Prefect original code.)

    Register a pre-serialized flow.

    Args:
        - client (prefect.Client): the prefect client
        - serialized_flow (dict): the serialized flow
        - project_id (str): the project id
        - force (bool, optional): If `False` (default), an idempotency key will
            be generated to avoid unnecessary re-registration. Set to `True` to
            force re-registration.
        - schedule (bool, optional): If `True` (default) activates the flow schedule
            upon registering.

    Returns:
        - flow_id (str): the flow id
        - flow_version (int): the flow version
        - is_new (bool): True if this is a new flow version, false if
            re-registration was skipped.
    """
    # Get most recent flow id for this flow. This can be removed once
    # the registration graphql routes return more information
    flow_name = serialized_flow["name"]
    resp = client.graphql(
        {
            "query": {
                with_args(
                    "flow",
                    {
                        "where": {
                            "_and": {
                                "name": {"_eq": flow_name},
                                "project": {"id": {"_eq": project_id}},
                            }
                        },
                        "order_by": {"version": EnumValue("desc")},
                        "limit": 1,
                    },
                ): {"id", "version"}
            }
        }
    )
    if resp.data.flow:
        prev_id = resp.data.flow[0].id
        prev_version = resp.data.flow[0].version
    else:
        prev_id = None
        prev_version = 0

    inputs = dict(
        project_id=project_id,
        serialized_flow=compress(serialized_flow),
        set_schedule_active=schedule,
    )
    if not force:
        inputs["idempotency_key"] = hashlib.sha256(
            json.dumps(serialized_flow, sort_keys=True).encode()
        ).hexdigest()

    res = client.graphql(
        {
            "mutation($input: create_flow_from_compressed_string_input!)": {
                "create_flow_from_compressed_string(input: $input)": {"id"}
            }
        },
        variables=dict(input=inputs),
        retry_on_api_error=False,
    )

    new_id = res.data.create_flow_from_compressed_string.id

    if new_id == prev_id:
        return new_id, prev_version, False
    return new_id, prev_version + 1, True


def filename_to_python_module(filename: Path) -> str:
    """
    Returns the Python module name from a filename.

    Example:

    - Filename:

    ```py
    path/to/file.py
    ```

    - Output:

    ```py
    'path.to.file'
    ```

    Args:
        filename (str): The filename to get the Python module name from.

    Returns:
        str: The Python module name.
    """
    # Get the file path in Python module format.
    return filename.with_suffix("").as_posix().replace("/", ".")


def get_declared(python_file: Path) -> list[str]:
    """
    Returns a list of declared variables, functions and classes
    in a Python file. The output must be fully qualified.

    Example:

    - Python file (path/to/file.py):

    ```py
    x = 1
    y = 2

    def func1():
        pass

    class Class1:
        pass
    ```

    - Output:

    ```py
    ['path.to.file.x', 'path.to.file.y', 'path.to.file.func1', 'path.to.file.Class1']
    ```

    Args:
        python_file (str): The Python file to get the declared variables from.

    Returns:
        list: A list of declared variables from the Python file.
    """
    # We need to get the contents of the Python file.
    content = python_file.read_text()

    # Get file path in Python module format.
    file_path = filename_to_python_module(python_file)

    # Parse it into an AST.
    tree = ast.parse(content)

    # Then, iterate over the imports.
    declared = []
    for node in tree.body:
        # print(type(node))
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    declared.append(f"{file_path}.{target.id}")
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                declared.append(f"{file_path}.{node.target.id}")
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                declared.append(f"{file_path}.{node.target.id}")
        elif isinstance(node, ast.With):
            for item in node.items:
                if isinstance(item, ast.withitem):
                    if isinstance(item.optional_vars, ast.Name):
                        declared.append(f"{file_path}.{item.optional_vars.id}")
        elif isinstance(node, ast.FunctionDef):
            declared.append(f"{file_path}.{node.name}")
        elif isinstance(node, ast.AsyncFunctionDef):
            declared.append(f"{file_path}.{node.name}")
        elif isinstance(node, ast.ClassDef):
            declared.append(f"{file_path}.{node.name}")

    return declared


def evaluate_declaration(declared: str) -> Any:
    """
    Evaluate declaration
    """
    return eval(declared, {"pipelines": importlib.import_module("pipelines")})


def get_affected_flows() -> list[FlowLike]:
    # dependent_files.txt is created by step code tree analysis
    dependent_files_txt = Path("dependent_files.txt")
    dependend_files = (
        [
            fname
            for fname in dependent_files_txt.read_text().splitlines()
            if fname.endswith(".py")
        ]
        if dependent_files_txt.exists()
        else []
    )

    flow_files = set()

    for file in dependend_files:
        flow_file = Path(file).parent / "flows.py"
        if flow_file.exists():
            flow_files.add(flow_file)

    declarations = []
    for flow_file in flow_files:
        declarations.extend(get_declared(flow_file))

    flows = []
    for declaration in declarations:
        try:
            evaluate = evaluate_declaration(declaration)
            if isinstance(evaluate, prefect.Flow):
                flows.append(evaluate)
        except Exception as e:
            logger.warning(f"Could not evaluate {declaration}: {e}")
    return flows


def has_dbt_related_files_changed(files: list[str]) -> bool:
    result = False

    for file in files:
        if file.endswith("schema.yml"):
            result = True
            break

        if file == "profiles.yml":
            result = True
            break

        if file == "dbt_project.yml":
            result = True
            break

        if file == "packages.yml":
            result = True
            break

        if file.startswith("models") and file.endswith(".sql"):
            result = True
            break

        if file.startswith("test-dbt") and file.endswith(".sql"):
            result = True
            break

        if file.startswith("macros") and file.endswith(".sql"):
            result = True
            break

    return result


def pipeline_project_file_relevant_changed(files: list[str]) -> bool:
    """
    Check if uv.lock or pyproject.toml is changed.

    Args:
        files (list[str]): Files changed.

    Returns:
        bool: Return true if uv.lock or pyproject.toml is changed.
    """
    result = False

    for file in files:
        if file == "uv.lock":
            result = True
            break
        if file == "pyproject.toml":
            result = True
            break

    return result


def main(
    project: str,
    path: str,
    max_retries: int = 5,
    retry_interval: int = 5,
    schedule: bool = False,
    filter_affected_flows: bool = False,
    modified_files: str | None = None,
) -> None:
    """
    A helper for registering Prefect flows. The original implementation does not
    attend to our needs, unfortunately, because of no retry policy.

    Args:
        - project (str): The project to register the flows to.
        - path (str): The paths to the flows to register.
        - max_retries (int, optional): The maximum number of retries to attempt.
        - retry_interval (int, optional): The number of seconds to wait between
    """

    # Expands paths to find all python files
    paths = expand_paths([path])

    # Gets the project ID
    client = prefect.Client()  # type: ignore
    project_id = get_project_id(client, project)

    # Collects flows from paths
    logger.info("Collecting flows...")
    source_to_flows = collect_flows(paths)

    modified_files_list = (
        [] if modified_files is None else modified_files.split(" ")
    )

    is_pipelines_project_file_relevant_changed = (
        pipeline_project_file_relevant_changed(modified_files_list)
    )

    # Filter affected flow if not important pipeline project file changed
    if (
        filter_affected_flows
        and not is_pipelines_project_file_relevant_changed
    ):
        # Filter out flows that are not affected by the change
        affected_flows = get_affected_flows()
        for key in source_to_flows.keys():
            filtered_flows = []
            for flow in source_to_flows[key]:
                if flow in affected_flows:
                    filtered_flows.append(flow)
            source_to_flows[key] = filtered_flows

    # Force registration of flow execute_dbt_model if some dbt related files is changed
    EXECUTE_DBT_MODEL_FLOW_NAME = "BD template: Executa DBT model"
    flow_execute_dbt_model_changed = (
        len(
            [
                flows
                for flows in source_to_flows.values()
                for flow in flows
                if flow.name == EXECUTE_DBT_MODEL_FLOW_NAME
            ]
        )
        > 0
    )

    dbt_related_files_has_modified = has_dbt_related_files_changed(
        modified_files_list
    )

    if not flow_execute_dbt_model_changed and dbt_related_files_has_modified:
        logger.warning(
            f"Registering {EXECUTE_DBT_MODEL_FLOW_NAME} because dbt related files is changed",
        )
        execute_dbt_model_flow = (
            Path("pipelines") / "utils" / "execute_dbt_model" / "flows.py"
        )
        evals_execute_dbt_model = [
            evaluate_declaration(declaration)
            for declaration in get_declared(execute_dbt_model_flow)
        ]
        for key in source_to_flows.keys():
            flows_instance = [
                e
                for e in evals_execute_dbt_model
                if isinstance(e, prefect.Flow)
            ]
            source_to_flows[key].extend(flows_instance)

    # Iterate through each file, building all storage and registering all flows
    # Log errors as they happen, but only exit once all files have been processed
    stats = Counter(registered=0, errored=0, skipped=0)
    for source, flows in source_to_flows.items():
        logger.info(f"Processing {source!r}:")
        stats += build_and_register(
            client,
            flows,
            project_id,
            max_retries=max_retries,
            retry_interval=retry_interval,
            schedule=schedule,
        )

    # Output summary message
    registered = stats["registered"]
    skipped = stats["skipped"]
    errored = stats["errored"]
    logger.info(
        f"Registered {registered} flows, skipped {skipped} flows, and errored {errored} flows."
    )

    # If not in a watch call, exit with appropriate exit code
    if stats["errored"]:
        raise Exception("One or more flows failed to register")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="register", description="Register a flow"
    )

    parser.add_argument(
        "--project", type=str, required=True, help="Name of project"
    )
    parser.add_argument(
        "--path", type=str, required=True, help="Path to pipelines"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        required=False,
        default=5,
        help="The maximum number of retries to attempt",
    )
    parser.add_argument(
        "--retry-interval",
        type=int,
        required=False,
        default=5,
        help="The number of seconds to wait between",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Schedule pipelines.",
    )
    parser.add_argument(
        "--filter-affected-flows",
        action="store_true",
        help="Filter affected flows to register.",
    )
    parser.add_argument(
        "--modified-files", type=str, required=False, help="Modified files"
    )

    args = parser.parse_args()

    main(
        project=args.project,
        path=args.path,
        max_retries=args.max_retries,
        retry_interval=args.retry_interval,
        schedule=args.schedule,
        filter_affected_flows=args.filter_affected_flows,
        modified_files=args.modified_files,
    )
