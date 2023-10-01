# -*- coding: utf-8 -*-
# fmt: off
from subprocess import run as _run


def run(*args):
    process = _run(*args)
    return process.returncode


def main():
    """Lint all python files in the project"""
    code = 0
    code |= run(["poetry", "check"])
    code |= run(["isort", "--profile", "black", "--skip", "pipelines/{{cookiecutter.project_name}}", "--check-only", "."])
    code |= run(["black", "--exclude", "pipelines/{{cookiecutter.project_name}}", "--check", "."])
    code |= run(["autoflake", "--exclude", "pipelines/{{cookiecutter.project_name}}", "--check", "--recursive", "--quiet", "."])
    code |= run(["flake8", "--exclude", "pipelines/{{cookiecutter.project_name}}", "."])
    exit(code)
