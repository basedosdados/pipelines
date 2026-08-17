"""Prefect 3 tasks for br_sfb_sicar — thin wrappers over utils.py.

Download and cleaning are split into two tasks that the flow interleaves per
UF x theme: download one zip, clean it to partitioned parquet, delete the zip.
Processing one state x theme at a time keeps peak disk near a single archive —
the APP theme alone is hundreds of MB per state.
"""

import os
import subprocess
import sys

from prefect import task
from SICAR import Sicar

from pipelines.constants import constants
from pipelines.crawler.sfb_sicar.utils import (
    release_dates_to_iso,
    retry_download_car,
)


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def get_release_dates_task() -> dict:
    """Return ``{uf: 'YYYY-MM-DD'}`` of per-UF snapshot (release) dates.

    Reads SICAR's release-dates page and normalizes ``dd/mm/yyyy`` to ISO. This
    is the per-UF snapshot that becomes the ``data`` partition value and, via its
    maximum, the source ``max_date`` for the poll.
    """
    return release_dates_to_iso(Sicar().get_release_dates())


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def download_uf_theme(
    input_dir: str,
    sigla_uf: str,
    polygon: str,
    tries: int,
    max_retries: int,
) -> str:
    """Download one UF x theme zip; return its path (``<UF>_<POLYGON>.zip``).

    Args:
        input_dir: Directory to download into (created if absent).
        sigla_uf: Two-letter UF code.
        polygon: SICAR Polygon enum value (e.g. ``"AREA_IMOVEL"``).
        tries: Captcha attempts per download.
        max_retries: Read-timeout retries wrapping the download.

    Returns:
        The downloaded zip's path.
    """
    os.makedirs(input_dir, exist_ok=True)
    retry_download_car(
        car=Sicar(),
        state=sigla_uf,
        polygon=polygon,
        folder=input_dir,
        tries=tries,
        max_retries=max_retries,
    )
    return os.path.join(input_dir, f"{sigla_uf}_{polygon}.zip")


@task
def clean_uf_theme(
    zip_path: str,
    output_dir: str,
    table: str,
    snapshot_iso: str,
    sigla_uf: str,
) -> int:
    """Clean one UF x theme zip into partitioned parquet, then delete the zip.

    Reads all ``.shp`` parts, drops foreign-UF rows, maps to the architecture's
    all-string columns, and appends the ``data``/``sigla_uf`` partition. The zip
    is removed afterwards to keep peak disk bounded.

    Args:
        zip_path: The downloaded ``<UF>_<POLYGON>.zip``.
        output_dir: Parquet output root (``<output_dir>/<table>/...``).
        table: Output table slug.
        snapshot_iso: Per-UF release date, ``'YYYY-MM-DD'``.
        sigla_uf: UF code.

    Returns:
        The number of rows written for this UF x theme.
    """
    # Run the clean in a child process launched with MALLOC_ARENA_MAX in its
    # environment. glibc reads that only before its first malloc, so it must be
    # present at process start — capping arenas here is what keeps the dense
    # Amazonas app clean from ballooning RSS and OOMing the worker. See
    # ``_clean_runner`` for the mechanism.
    env = {
        **os.environ,
        "MALLOC_ARENA_MAX": "2",
        "MALLOC_TRIM_THRESHOLD_": "131072",
    }
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipelines.crawler.sfb_sicar._clean_runner",
            zip_path,
            output_dir,
            table,
            snapshot_iso,
            sigla_uf,
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    print(proc.stdout, end="")
    if proc.returncode != 0:
        raise RuntimeError(
            f"clean failed for {sigla_uf} {table}:\n{proc.stderr}"
        )
    # Parse "RESULT rows=<n> dropped=<n> peak_rss_mb=<n>".
    result = next(
        ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")
    )
    fields = dict(tok.split("=") for tok in result.split()[1:])
    n, dropped = int(fields["rows"]), int(fields["dropped"])
    print(
        f"{sigla_uf} {table}: rows={n} dropped_foreign_uf={dropped} "
        f"snapshot={snapshot_iso} peak_rss_mb={fields['peak_rss_mb']}"
    )
    if os.path.exists(zip_path):
        os.remove(zip_path)
    return n
