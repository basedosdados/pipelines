# -*- coding: utf-8 -*-
import ast
import sys
from pathlib import Path
from typing import List, Tuple, Union

import networkx as nx
import yaml
from prefect import Flow

message_id = 0


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


def python_module_to_filename(python_module: str) -> str:
    """
    Returns the filename from a Python module.

    Example:

    - Python module:

    ```py
    'path.to.file'
    ```

    - Output:

    ```py
    'path/to/file.py'
    ```

    Args:
        python_module (str): The Python module to get the filename from.

    Returns:
        str: The filename from the Python module.
    """
    # Get the file path in Python module format.
    file_path = Path(python_module).with_suffix("").as_posix().replace(".", "/")

    return f"{file_path}.py"


def get_dependencies(python_file: Union[str, Path]) -> List[str]:
    """
    Returns a list of dependencies from a Python file. The dependencies are
    defined as the import statements in the file. Their names on the output
    must be fully qualified.

    Example:

    - Python file:

    ```py
    from prefect import task
    from prefect.tasks.secrets import Secret
    from some_package import (
        func1, func2,
    )
    ```

    - Output:

    ```py
    ['prefect.task', 'prefect.tasks.secrets.Secret', 'some_package.func1', 'some_package.func2']
    ```

    Args:
        python_file (str): The Python file to get the dependencies from.

    Returns:
        list: A list of dependencies from the Python file.
    """
    # We need to get the contents of the Python file.
    with open(python_file, "r") as f:
        content = f.read()

    # Parse it into an AST.
    tree = ast.parse(content)

    # Then, iterate over the imports.
    dependencies = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            for name in node.names:
                full_name = f"{name.name}"
                dependencies.append(full_name)
        elif isinstance(node, ast.ImportFrom):
            for name in node.names:
                full_name = f"{node.module}.{name.name}"
                dependencies.append(full_name)

    return dependencies


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


    if not Path(python_file).exists():
        log(f"{python_file} does not exist")
        return []

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


def list_all_python_files(directory: Union[str, Path]) -> List[Path]:
    """
    Returns a list of all Python files in a directory.

    Example:

    - Directory:

    ```py
    path/to/file1.py
    path/to/file2.py
    path/to/file3.py
    ```

    - Output:

    ```py
    ['path/to/file1.py', 'path/to/file2.py', 'path/to/file3.py']
    ```

    Args:
        directory (str): The directory to list the files from.

    Returns:
        list: A list of all Python files in the directory.
    """
    # Get the directory path.
    directory = Path(directory)

    # Get all files in the directory.
    files = directory.glob("**/*.py")

    # Filter out files that are not Python files.
    files = [f for f in files if f.suffix == ".py"]

    return [f.as_posix() for f in files]


def object_is_instance(fully_qualified_import: str, compare_to: type) -> bool:
    """
    Returns whether an object is an instance of a class.

    Args:
        fully_qualified_import (str): The fully qualified import to check.
        compare_to (type): The type to compare the import to.

    Returns:
        bool: Whether the object is an instance of the class.
    """
    # Get the module and class name.
    module, class_name = fully_qualified_import.rsplit(".", 1)

    # Import the module.
    module = __import__(module, fromlist=[class_name])

    # Get the object.
    object_ = getattr(module, class_name)

    # Check if the object is an instance of the class.
    return isinstance(object_, compare_to)


def assert_all_imports_are_declared(root_directory: str) -> None:
    """
    Asserts that all imports are declared somewhere.
    """
    # Get all Python files.
    files = [
        file_
        for file_ in list_all_python_files(root_directory)
        if "cookiecutter" not in file_
    ]

    # Get all declared stuff.
    declared = set()
    for file_ in files:
        file_declared = [
            item
            for item in get_declared(file_)
            if (item.startswith("pipelines") and not item.endswith("*"))
        ]
        declared.update(file_declared)

    # Get all dependencies.
    dependencies = set()
    for file_ in files:
        file_dependencies = [
            item
            for item in get_dependencies(file_)
            if (item.startswith("pipelines") and not item.endswith("*"))
        ]
        dependencies.update(file_dependencies)

    # Assert that all dependencies are declared.
    for dependency in dependencies:
        assert dependency in declared, f"{dependency} is not declared."


def build_dependency_graph(root_directory: str) -> nx.DiGraph:
    """
    Builds a dependency graph from a directory.

    Args:
        root_directory (str): The directory to build the graph from.

    Returns:
        nx.DiGraph: The dependency graph.
    """
    # Get all Python files.
    files = [
        file_
        for file_ in list_all_python_files(root_directory)
        if "cookiecutter" not in file_
    ]

    # Get dependencies by file.
    dependencies_by_file = {}
    for file_ in files:
        file_dependencies = set(
            [item for item in get_dependencies(file_) if item.startswith("pipelines")]
        )
        dependencies_by_file[file_] = file_dependencies

    # Get declared stuff by file.
    declared_by_file = {}
    for file_ in files:
        file_declared = set(get_declared(file_))
        declared_by_file[file_] = file_declared

    # Get all declared.
    all_declared = set()
    for file_ in files:
        file_declared = set(get_declared(file_))
        all_declared.update(file_declared)

    # Build the dependency graph.
    graph = nx.DiGraph()

    # First we add the dependencies. Each dependency neighbor is a file that
    # depends on it.
    for file_ in files:
        if file_ not in graph.nodes:
            graph.add_node(file_)
        for dependency in dependencies_by_file[file_]:
            if dependency.endswith("*"):
                for sub_dependency in all_declared:
                    if sub_dependency.startswith(dependency[:-1]):
                        if sub_dependency not in graph.nodes:
                            graph.add_node(sub_dependency)
                        graph.add_edge(sub_dependency, file_)
            else:
                if dependency not in graph.nodes:
                    graph.add_node(dependency)
                graph.add_edge(dependency, file_)

    # Then we add the declared stuff. Each file neighbor is a declared thing.
    for file_ in files:
        if file_ not in graph.nodes:
            graph.add_node(file_)
        for dependency in declared_by_file[file_]:
            if dependency not in graph.nodes:
                graph.add_node(dependency)
            graph.add_edge(file_, dependency)

    return graph


def check_for_variable_name_conflicts(
    changed_files: List[str], root_directory: str
) -> List[Tuple[str, str]]:
    """
    Checks if there will be any conflicts with variable names.
    """
    # Get all Python files.
    files = [
        file_
        for file_ in list_all_python_files(root_directory)
        if "cookiecutter" not in file_
    ]

    # Remove all changed files from the list of files.
    files = [file_ for file_ in files if file_ not in changed_files]

    # Get all declared things in the changed files.
    declared_changed = set()
    for file_ in files:
        if file_ in changed_files:
            file_declared = set(get_declared(file_))
            declared_changed.update(file_declared)

    # Get all declared things in the remaining files.
    declared_remaning = set()
    for file_ in files:
        file_declared = set(get_declared(file_))
        declared_remaning.update(file_declared)

    # Filter out what is not a Flow.
    declared_changed = [
        obj for obj in declared_changed if object_is_instance(obj, Flow)
    ]
    declared_remaning = [
        obj for obj in declared_remaning if object_is_instance(obj, Flow)
    ]

    # Check for conflicts.
    conflicts = []
    for changed in declared_changed:
        for remaining in declared_remaning:
            if changed.split(".")[-1] == remaining.split(".")[-1]:
                conflicts.append((changed, remaining))

    # Return the conflicts.
    return conflicts


def log(message: str):
    """
    Logs a message to the output of a GitHub Action.
    """
    message = message.replace("\n", "%0A")
    print(f"::set-output name=pr-message::{message}")


def identify_code_owners(files: List[str]) -> List[str]:
    """
    Identifies the code owners in order to warn them.
    """
    # Load the code owners YAML file.
    with open("code_owners.yaml") as file_:
        code_owners = yaml.safe_load(file_)

    # Get the code owners.
    owners = set()
    for file_ in files:
        path_parts = file_.split("/")
        section = code_owners
        for i in range(len(path_parts) - 1):
            if path_parts[i] in section:
                current_owners = section[path_parts[i]]["owners"]
                if "dir" in section[path_parts[i]]:
                    section = section[path_parts[i]]["dir"]
            else:
                break
        owners.update(current_owners)

    # Return the owners.
    return list(owners)


if __name__ == "__main__":
    # Assert arguments.
    if len(sys.argv) not in [2, 3]:
        print(f"Usage: python {sys.argv[0]} <changed_files> [--write-to-file]")

    # Write to file?
    write_to_file = "--write-to-file" in sys.argv

    # Get modified files
    changed_files: List[str] = sys.argv[1].split(" ")
    print("These are all the changed files:")
    for file_ in changed_files:
        print(f"\t- {file_}")

    # Filter out non-Python and non-pipelines files.
    changed_files = [
        file_
        for file_ in changed_files
        if file_.endswith(".py")
        and file_.startswith("pipelines")
        and "cookiecutter" not in file_
    ]
    print("We're interested in these files:")
    for file_ in changed_files:
        print(f"\t- {file_}")

    # Build the dependency graph.
    graph = build_dependency_graph("pipelines/")

    # Get all declarations that the exported files export.
    exported_declarations = set()
    for file_ in changed_files:
        exported_declarations.update(get_declared(file_))
    print("These files export these declarations:")
    for declaration in exported_declarations:
        print(f"\t- {declaration}")

    # Get all files that depend on the exported declarations.
    dependent_files = set()
    for declaration in exported_declarations:
        dependent_files.update(graph.successors(declaration))
    if "pipelines/flows.py" in dependent_files:
        dependent_files.remove("pipelines/flows.py")
    print("These files depend on the exported declarations:")
    for file_ in dependent_files:
        print(f"\t- {file_}")

    # Write dependent file list to file.
    if write_to_file:
        with open("dependent_files.txt", "w") as f:
            for file_ in dependent_files:
                f.write(f"{file_}\n")

    # Start a PR message
    message = "### Análise da árvore de código\n\n"

    # Format a message for the files that depend on the exported declarations.
    if len(dependent_files) > 0:
        message += "**Os seguintes arquivos são afetados diretamente por alterações "
        message += "realizadas nesse pull request:**"
        for file_ in dependent_files:
            message += f"\n\t- {file_}"
        message += "\n\n"

    code_owners = identify_code_owners(changed_files)
    print("These are the code owners:")
    for owner in code_owners:
        print(f"\t- {owner}")

    if len(code_owners) > 0:
        message += "**Os seguintes usuários devem ser avisados sobre a alteração:**"
        for owner in code_owners:
            message += f"\n\t- @{owner}"
        message += "\n\n"

    # Check for variable name conflicts.
    conflicts = check_for_variable_name_conflicts(changed_files, "pipelines/")
    if len(conflicts) > 0:
        message += (
            "**Existem conflitos entre nomes de variáveis nos seguintes objetos:**"
        )
        for conflict in conflicts:
            message += "\n\t- {conflict[0]} e {conflict[1]}"
        message += "\n\n"

    # If there is nothing wrong, let'em know!
    if len(dependent_files) == 0 and len(conflicts) == 0:
        message += "*Nenhum problema encontrado!*"

    if not write_to_file:
        log(message)

    # Raise if there are conflicts
    if len(conflicts) > 0:
        raise Exception("There are variable name conflicts!")
