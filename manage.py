from os import path
from pathlib import Path
import pkgutil
from sys import exit
from uuid import uuid4

import typer
from cookiecutter.main import cookiecutter
from loguru import logger
from string_utils import is_snake_case

CONSTANTS_FILE_PATH = Path(__file__).parent / "pipelines" / "constants.py"
CONSTANTS_FIND_AGENTS_TEXT = "M9w=k-b_\n"
COOKIECUTTER_PATH_AGENCY = Path(__file__).parent / "pipelines"
COOKIECUTTER_PATH_PROJECT = Path(
    __file__).parent / "pipelines" / "{{cookiecutter.project_name}}"
FLOWS_FILE_PATH = Path(__file__).parent / "pipelines" / "flows.py"
PIPELINES_PATH = Path(__file__).parent / "pipelines"

app = typer.Typer()


@logger.catch
def append_text(original_text: str, text_to_add: str, after_text: str = None):
    """
    Appends text to a string after a given text.

    :param original_text: The original text.
    :param text_to_add: The text to add.
    :param after_text: The text after which the text to add should be added.
    :return: The original text with the text to add appended.
    """
    if after_text is None:
        return original_text + text_to_add
    else:
        return original_text.replace(after_text, after_text + text_to_add)


@logger.catch
def add_import(project_name: str):
    """
    Adds an import to the project's flows file.

    :param project_name: The project name.
    :return: The original text with the import appended.
    """
    with open(FLOWS_FILE_PATH, "r") as flows_file:
        flows_text = flows_file.read()

    flows_text = append_text(
        flows_text, f"from pipelines.{project_name}.flows import *\n")

    with open(FLOWS_FILE_PATH, "w") as flows_file:
        flows_file.write(flows_text)


@logger.catch
def add_agency_import(agency_name: str, project_name: str):
    """
    Adds an import to the agency's __init__ file.

    :param agency_name: The agency name.
    :param project_name: The project name.
    :return: The original text with the import appended.
    """
    path = PIPELINES_PATH / agency_name / "__init__.py"
    with open(path, "r") as flows_file:
        flows_text = flows_file.read()

    flows_text = append_text(
        flows_text, f"from pipelines.{agency_name}.{project_name}.flows import *\n")

    with open(path, "w") as flows_file:
        flows_file.write(flows_text)


@logger.catch
def add_agent(project_name: str):
    """
    Adds the agent to the constants file.

    :param project_name: The name of the project.
    """
    with open(CONSTANTS_FILE_PATH, "r") as file:
        file_text = file.read()

    file_text = append_text(
        file_text, f"    {project_name.upper()}_AGENT_LABEL = \"{uuid4()}\"\n", CONSTANTS_FIND_AGENTS_TEXT)

    with open(CONSTANTS_FILE_PATH, "w") as file:
        file.write(file_text)


@logger.catch
def cut_agency_cookie(agency_name: str):
    """
    Runs the cookiecutter command.

    :param project_name: The name of the project.
    """
    cookiecutter(
        str(COOKIECUTTER_PATH_AGENCY),
        no_input=True,
        extra_context={
            "project_name": agency_name,
            "workspace_name": "example",
        },
        output_dir=str(COOKIECUTTER_PATH_AGENCY)
    )
    delete_file(COOKIECUTTER_PATH_AGENCY / agency_name / "cookiecutter.json")


@logger.catch
def cut_project_cookie(agency_name: str, project_name: str):
    """
    Runs the cookiecutter command.

    :param project_name: The name of the project.
    """
    cookiecutter(
        str(COOKIECUTTER_PATH_PROJECT),
        no_input=True,
        extra_context={
            "project_name": agency_name,
            "workspace_name": project_name,
        },
        output_dir=str(COOKIECUTTER_PATH_AGENCY / agency_name)
    )


@logger.catch
def delete_file(fname: str):
    """
    Deletes a file.

    :param fname: The name of the file.
    """
    if path.exists(fname):
        Path(fname).unlink()


@logger.catch
def single_word_valid(input: str) -> bool:
    """
    - Must have only letters and numbers
    - Must not start with a number
    """
    return input.isalnum() and not input[0].isdigit()


@logger.catch
def name_already_exists(input: str) -> bool:
    """
    Checks if the name already exists.

    :param input: The input.
    :return: True if the name already exists, False otherwise.
    """
    try:
        import pipelines
    except ImportError:
        logger.error(
            "Não foi possível importar as pipelines."
        )
        exit(1)
    pkgpath = path.dirname(pipelines.__file__)
    return input in [name for _, name, _ in pkgutil.iter_modules([pkgpath])]


@logger.catch
def check_name(input: str) -> str:
    """
    Checks if the name is valid.

    :param input: The input.
    :return: The input if it is valid, otherwise an error message.
    """
    # Must be either valid as a single word or a snake case name
    valid = single_word_valid(input) or is_snake_case(input)

    # Also, must not already exist
    valid = valid and not name_already_exists(input)

    if valid:
        return input
    else:
        logger.error(
            f"O nome {input} é inválido. Ele deve estar em snake_case e ser único"
        )
        exit(1)


@logger.catch
def agency_must_exist(input: str) -> str:
    """
    Checks whether the agency exists.

    :param input: The input.
    :return: The input if it exists, otherwise an error message.
    """
    try:
        import pipelines
    except ImportError:
        logger.error(
            "Não foi possível importar as pipelines."
        )
        exit(1)
    pkgpath = path.dirname(pipelines.__file__)
    agencies = [name for _, name, _ in pkgutil.iter_modules([pkgpath])]
    if input in agencies:
        return input
    else:
        logger.error(
            f"A agência {input} não existe."
        )
        exit(1)


@logger.catch
def project_must_not_exist_in_agency(input: str, agency: str) -> str:
    """
    Checks whether the project exists in the agency.

    :param input: The input.
    :param agency: The agency.
    :return: The input if it exists, otherwise an error message.
    """
    try:
        import pipelines
    except ImportError:
        logger.error(
            "Não foi possível importar as pipelines."
        )
        exit(1)
    pkgpath = path.dirname(pipelines.__file__)
    agencies = [name for _, name, _ in pkgutil.iter_modules([pkgpath])]
    if agency in agencies:
        projects = [name for _, name,
                    _ in pkgutil.iter_modules([str(Path(pkgpath) / agency)])]
        if input in projects:
            logger.error(
                f"O projeto {input} já existe na agência {agency}."
            )
            exit(1)
        return input
    else:
        logger.error(
            f"O órgão {agency} não foi encontrado."
        )
        exit(1)


@app.command()
def add_agency(agency_name: str = typer.Argument(..., callback=check_name)):
    """
    Cria um novo órgão usando o cookiecutter.
    """
    add_agent(agency_name)
    add_import(agency_name)
    cut_agency_cookie(agency_name)


@app.command()
def add_project(
    agency_name: str = typer.Argument(..., callback=agency_must_exist),
    project_name: str = typer.Argument(...)
):
    """
    Cria um novo projeto no órgão usando o cookiecutter.
    """
    project_must_not_exist_in_agency(project_name, agency_name)
    add_agency_import(agency_name, project_name)
    cut_project_cookie(agency_name, project_name)


@ app.command()
def list_projects():
    """
    Lista os projetos cadastrados e nomes reservados
    """
    try:
        import pipelines
    except ImportError:
        logger.error(
            "Não foi possível importar as pipelines."
        )
        exit(1)
    pkgpath = path.dirname(pipelines.__file__)
    logger.info(
        "Projetos cadastrados e nomes reservados:")
    for _, name, _ in pkgutil.iter_modules([pkgpath]):
        logger.info(f"- {name}")


if __name__ == "__main__":
    app()
