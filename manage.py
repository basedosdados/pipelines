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
COOKIECUTTER_PATH = Path(__file__).parent / "pipelines"
FLOWS_FILE_PATH = Path(__file__).parent / "pipelines" / "flows.py"

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
def cut_cookie(project_name: str):
    """
    Runs the cookiecutter command.

    :param project_name: The name of the project.
    """
    cookiecutter(
        str(COOKIECUTTER_PATH),
        no_input=True,
        extra_context={
            "project_name": project_name,
        },
        output_dir=str(COOKIECUTTER_PATH)
    )


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


@app.command()
def add_project(project_name: str = typer.Argument(..., callback=check_name)):
    """
    Cria um novo projeto usando o cookiecutter.
    """
    add_agent(project_name)
    add_import(project_name)
    cut_cookie(project_name)


@app.command()
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
