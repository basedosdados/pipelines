"""
Tasks for br_tse_eleicoes (Prefect 3).

This module holds only the *orchestration* tasks. The actual data cleaning is
delegated to the builders in ``models/br_tse_eleicoes/code/python`` via
:func:`run_build_step`. Upload and dbt are reused from
``pipelines.utils.tasks`` (``upload_to_gcs``, ``run_dbt``).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from prefect import task

from pipelines.br_tse_eleicoes.constants import constants


@task(retries=1, retry_delay_seconds=30)
def run_build_step(step: str) -> str:
    """Run one ``build.py`` step from the data-cleaning code and return the
    output directory.

    ``step`` is a ``build.py`` step name (e.g. ``candidates``,
    ``results_mun_zone``, ``normalize``, ``aggregate``). The subprocess writes
    Hive-partitioned parquets under ``$TSE_DATA_DIR/output_python``.

    Several producer tables share one build step (e.g. ``campaign_finance``
    builds bens/receitas/despesas); the orchestrator is responsible for not
    invoking the same step redundantly.
    """
    code_dir: Path = constants.CODE_DIR.value
    data_dir = os.environ.get("TSE_DATA_DIR")
    if not data_dir:
        raise OSError(
            "TSE_DATA_DIR is not set — required to locate raw input/output."
        )

    cmd = ["uv", "run", "python", "build.py", step]
    print(f"build | running {' '.join(cmd)} (cwd={code_dir})")
    result = subprocess.run(
        cmd,
        cwd=str(code_dir),
        env={**os.environ, "TSE_DATA_DIR": data_dir},
        capture_output=True,
        text=True,
    )
    print(result.stdout[-4000:])
    if result.returncode != 0:
        print(result.stderr[-4000:])
        raise RuntimeError(
            f"build.py {step} failed (exit {result.returncode})"
        )

    return str(Path(data_dir) / "output_python")


def output_path_for(table_id: str) -> str:
    """Partitioned-parquet directory a producer/aggregation table uploads."""
    data_dir = os.environ.get("TSE_DATA_DIR", "")
    return str(Path(data_dir) / "output_python" / table_id)
