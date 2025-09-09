import argparse
from pathlib import Path

from cookiecutter.main import cookiecutter
from string_utils import is_snake_case

PIPELINES_DIR = Path(__file__).parent / "pipelines"

COOKIECUTTER_PIPELINE_FOLDER = PIPELINES_DIR / "{{cookiecutter.pipeline_name}}"

DATASETS_INIT_FILE = PIPELINES_DIR / "datasets" / "__init__.py"


def list_pipelines() -> list[str]:
    """
    List all pipelines
    """

    return [
        path.stem
        for path in (PIPELINES_DIR / "datasets").iterdir()
        if path.is_dir() and path.name != "__pycache__" and path
    ]


def add_pipeline(name: str) -> None:
    """
    Create a new pipeline template
    """
    if name in list_pipelines():
        raise Exception(f"Pipeline `{name}` already exists")

    assert name[0].isalnum() and is_snake_case(name), (
        f"Invalid pipeline name `{name}`. Pipeline name format: First letter should be a letter and name should be snake_case format"
    )

    cookiecutter(
        template=COOKIECUTTER_PIPELINE_FOLDER.as_posix(),
        no_input=True,
        extra_context={
            "pipeline_name": name,
        },
        directory=PIPELINES_DIR.as_posix(),  # directory cookiecutter folder
        output_dir=(PIPELINES_DIR / "datasets").as_posix(),
    )

    flows_text = DATASETS_INIT_FILE.read_text()

    DATASETS_INIT_FILE.write_text(
        "\n".join([flows_text, f"from pipelines.{name}.flows import *"])
    )

    print(f"Pipeline {name} created.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="manage", description="Manage pipelines"
    )
    subparsers = parser.add_subparsers(dest="command")

    # pipelines command
    parser_add = subparsers.add_parser(
        "add-pipeline", help="Add a new pipeline"
    )
    parser_add.add_argument("name", help="Name of the pipeline to add")

    subparsers.add_parser("list-pipelines", help="List all pipelines")

    args = parser.parse_args()

    if args.command == "add-pipeline":
        add_pipeline(args.name.strip())
    elif args.command == "list-pipelines":
        print("\n".join(list_pipelines()))
    else:
        parser.print_help()
