# -*- coding: utf-8 -*-
from subprocess import run as _run


def run(*args):
    process = _run(*args)
    return process.returncode


def main():
    """Lint all python files in the project"""
    code = 0
    code |= run(["poetry", "check"])
    code |= run(["ruff", "check", "."])
    code |= run(["ruff", "format", "."])
    exit(code)
