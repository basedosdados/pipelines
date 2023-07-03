# -*- coding: utf-8 -*-
"""
Custom script for registering flows.
"""

import ast
from collections import (
    Counter,
    defaultdict,
)
import glob
import hashlib
import importlib
import json
import os
from pathlib import Path
import runpy
import sys
from time import sleep
import traceback
from typing import (
    Dict,
    List,
    Tuple,
    Union,
)

import box
from loguru import logger
import prefect
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.utilities.graphql import (
    compress,
    EnumValue,
    with_args,
)
from typer import Typer

import pipelines  # DO NOT REMOVE THIS LINE


app = Typer()
FlowLike = Union[box.Box, "prefect.Flow"]


def build_and_register(  # pylint: disable=too-many-branches
    client: "prefect.Client",
    flows: "List[FlowLike]",
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
                            logger.error(f"Retrying in {retry_interval} seconds...")
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
                    logger.warning(" Skipped (metadata unchanged)", fg="yellow")
                    stats["skipped"] += 1
    return stats


def collect_flows(
    paths: List[str],
) -> Dict[str, List[FlowLike]]:
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


def expand_paths(paths: List[str]) -> List[str]:
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
                    e.path for e in directory if e.is_file() and e.path.endswith(".py")
                )
        else:
            out.append(path)
    return out


def get_project_id(client: "prefect.Client", project: str) -> str:
    """
    (Adapted from Prefect original code.)

    Get a project id given a project name.

    Args:
        - project (str): the project name

    Returns:
        - str: the project id
    """
    resp = client.graphql(
        {"query": {with_args("project", {"where": {"name": {"_eq": project}}}): {"id"}}}
    )
    if resp.data.project:
        return resp.data.project[0].id
    raise Exception(f"Project {project!r} does not exist")


def load_flows_from_script(path: str) -> "List[prefect.Flow]":
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
        with prefect.context({"loading_flow": True, "local_script_path": abs_path}):
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


def prepare_flows(flows: "List[FlowLike]") -> None:
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
                new_labels = set(flow["environment"].get("labels") or []).union(labels)
                flow["environment"]["labels"] = sorted(new_labels)
            else:
                new_labels = set(flow["run_config"].get("labels") or []).union(labels)
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
    client: "prefect.Client",
    serialized_flow: dict,
    project_id: str,
    force: bool = False,
    schedule: bool = True,
) -> Tuple[str, int, bool]:
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


def filename_to_python_module(filename: str) -> str:
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
    file_path = Path(filename).with_suffix("").as_posix().replace("/", ".")

    return file_path


def get_declared(python_file: Union[str, Path]) -> List[str]:
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
    with open(python_file, "r") as f:
        content = f.read()

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


def get_affected_flows(fpath: str = None):
    if not fpath:
        fpath = "dependent_files.txt"
    with open(fpath, "r") as f:
        fnames = f.read().splitlines()
    fnames = [fname for fname in fnames if fname.endswith(".py")]
    flow_files = set()
    for fname in fnames:
        flow_file = Path(fname).parent / "flows.py"
        if flow_file.exists():
            flow_files.add(flow_file)
    declared_flows = []
    for flow_file in flow_files:
        declared_flows.extend(get_declared(flow_file))
    flows = []
    for flow in declared_flows:
        try:
            flows.append(eval(flow))
        except Exception:
            logger.warning(f"Could not evaluate {flow}")
    return flows


@app.command(name="register", help="Register a flow")
def main(
    project: str = None,
    path: str = None,
    max_retries: int = 5,
    retry_interval: int = 5,
    schedule: bool = True,
    filter_affected_flows: bool = False,
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

    if not (project and path):
        raise ValueError("Must specify a project and path")

    # Expands paths to find all python files
    paths = expand_paths([path])

    # Gets the project ID
    client = prefect.Client()
    project_id = get_project_id(client, project)

    # Collects flows from paths
    logger.info("Collecting flows...")
    source_to_flows = collect_flows(paths)

    if filter_affected_flows:
        # Filter out flows that are not affected by the change
        affected_flows = get_affected_flows("dependent_files.txt")
        for key in source_to_flows.keys():
            filtered_flows = []
            for flow in source_to_flows[key]:
                if flow in affected_flows:
                    filtered_flows.append(flow)
            source_to_flows[key] = filtered_flows

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
        f"Registered {registered} flows, skipped {skipped} flows, "
        f"and errored {errored} flows."
    )

    # If not in a watch call, exit with appropriate exit code
    if stats["errored"]:
        raise Exception("One or more flows failed to register")


if __name__ == "__main__":
    app()
