"""
Abstração do engine dbt.

Suporta dois engines durante a migração faseada:

- ``core``   → dbt-core (API programática ``dbtRunner``, engine atual de produção)
- ``fusion`` → dbt Fusion engine (binário Rust, invocado via subprocess)

O engine é escolhido em runtime pela env ``DBT_ENGINE`` (default ``core``).
Essa é a única chave para promover o Fusion à produção quando o adapter
BigQuery do Fusion for a GA: basta definir ``DBT_ENGINE=fusion`` no worker.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any


def _engine() -> str:
    return os.getenv("DBT_ENGINE", "core").strip().lower()


def _fusion_bin() -> str:
    return os.getenv("DBT_FUSION_BIN", "dbtf")


def build_cli_args(
    cmd: str,
    selected: str,
    target: str,
    flags: str | None,
    vars_dict: dict[str, Any] | None,
) -> list[str]:
    """Monta a lista de argumentos do CLI, idêntica para os dois engines."""
    cli_args = [cmd, "--select", selected, "--target", target]
    if flags and flags.startswith("--full-refresh") and cmd == "run":
        cli_args.insert(1, "--full-refresh")
    elif flags:
        cli_args.extend(flags.split())
    if vars_dict:
        cli_args.extend(["--vars", json.dumps(vars_dict)])
    return cli_args


def run_dbt_command(
    cmd: str,
    selected: str,
    target: str,
    flags: str | None = None,
    vars_dict: dict[str, Any] | None = None,
) -> None:
    """Executa um comando dbt (``run``/``test``) no engine selecionado.

    Levanta ``Exception`` em caso de falha, preservando o comportamento
    anterior baseado em ``dbtRunner``.
    """
    cli_args = build_cli_args(cmd, selected, target, flags, vars_dict)
    if _engine() == "fusion":
        _run_fusion(cli_args, cmd, selected, target)
    else:
        _run_core(cli_args, cmd, selected, target)


def _run_core(
    cli_args: list[str], cmd: str, selected: str, target: str
) -> None:
    # Import tardio: em modo fusion-only o dbt-core pode nem estar instalado.
    from dbt.cli.main import dbtRunner

    print(f"dbt (core) {' '.join(cli_args)}")
    result = dbtRunner().invoke(cli_args)
    if result.exception:
        raise Exception(f"dbt {cmd} exception: {result.exception}")
    if not result.success:
        for event in result.result or []:
            print(f"dbt | {getattr(event, 'message', event)}")
        raise Exception(f"dbt {cmd} falhou para {selected} (target={target})")
    print(f"dbt {cmd} OK: {selected} (target={target})")


def _run_fusion(
    cli_args: list[str], cmd: str, selected: str, target: str
) -> None:
    full_cmd = [_fusion_bin(), *cli_args]
    print(f"dbt (fusion) {' '.join(full_cmd)}")
    proc = subprocess.run(full_cmd, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode != 0:
        raise Exception(
            f"dbt {cmd} falhou para {selected} "
            f"(target={target}, exit={proc.returncode})"
        )
    print(f"dbt {cmd} OK: {selected} (target={target})")
