# -*- coding: utf-8 -*-
"""
Opens the `constants.py` file and updates the
`DOCKER_TAG` variable with the provided argument.
"""

from pathlib import Path
from sys import argv, exit
from typing import List

FILE_PATH = Path("./pipelines/constants.py")
REPLACE_TAG = "AUTO_REPLACE_DOCKER_TAG"
REPLACE_IMAGE = "AUTO_REPLACE_DOCKER_IMAGE"


def get_name_version_from_args() -> List[str]:
    """
    Returns the version from the command line arguments.
    """
    if len(argv) != 3:
        print("Usage: replace_docker_tag.py <image_name> <version>")
        exit(1)
    return argv[1], argv[2]


def replace_in_text(orig_text: str, find_text: str, replace_text: str) -> str:
    """
    Replaces the `find_text` with `replace_text` in the `orig_text`.
    """
    return orig_text.replace(find_text, replace_text)


def update_file(file_path: Path, image_name: str, version: str) -> None:
    """
    Updates the `DOCKER_TAG` variable in the `constants.py` file.
    """
    with file_path.open("r") as file:
        text = file.read()
    text = replace_in_text(text, REPLACE_TAG, version)
    text = replace_in_text(text, REPLACE_IMAGE, image_name)
    with file_path.open("w") as file:
        file.write(text)


if __name__ == "__main__":
    image_name, version = get_name_version_from_args()
    update_file(FILE_PATH, image_name, version)
