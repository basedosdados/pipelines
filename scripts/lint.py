# -*- coding: utf-8 -*-
from subprocess import run as _run


def run(*args):
    process = _run(*args)
    return process.returncode


def main():
    """Lint all python files in the project"""
    code = 0
    code |= run(["poetry", "check"])
    code |= run(["black", "--check", "."])
    code |= run(["isort", "--check-only", "."])
    code |= run(["autoflake", "--check", "--recursive", "--quiet", "."])
    code |= run(["flake8", "--exclude", "pipelines/{{cookiecutter.project_name}}", "."])
    exit(code)
