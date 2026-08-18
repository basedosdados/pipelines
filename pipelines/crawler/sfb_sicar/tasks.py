"""Prefect 3 tasks for br_sfb_sicar — thin wrappers over utils.py.

Download and cleaning are split into two tasks that the flow interleaves per
UF x theme: download one zip, clean it to partitioned parquet, delete the zip.
Processing one state x theme at a time keeps peak disk near a single archive —
the APP theme alone is hundreds of MB per state.
"""

import os
import shutil
import subprocess
import sys
import tempfile

from prefect import task
from SICAR import Sicar

from pipelines.constants import constants
from pipelines.crawler.sfb_sicar.utils import (
    extract_theme_zip,
    partition_dir,
    plan_shp_ranges,
    release_dates_to_iso,
    retry_download_car,
)

# Raw-geometry budget per range. Each range is cleaned in its own subprocess
# that exits afterwards, so this bounds a single process's peak memory, not an
# accumulating one. Measured on the worker, the per-range peak is ~8 GB whether
# the budget is 96 or 256 MB: it is dominated not by the aggregate range size but
# by a single pathologically dense Amazonas app feature (a river-network
# multipolygon) whose reproject + WKT serialization lands in whatever range holds
# it. Shrinking the budget only multiplies subprocess spawns for no memory gain,
# so keep it large; the ~8 GB peak fits the 12 GiB pod (which the successful runs
# already used).
CLEAN_BUDGET_BYTES = 256 * 1024 * 1024


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
    # Extract once, then clean each geometry-budgeted feature range in its OWN
    # short-lived child process. When each child exits the OS reclaims all its
    # memory, so glibc heap fragmentation / retaining arenas cannot accumulate
    # across ranges the way they do in one long-lived process (which OOMed the
    # 32 GiB worker on dense Amazonas app). Peak RSS is permanently one range.
    # MALLOC_ARENA_MAX is set in the child env too as a cheap second bound.
    part_dir = partition_dir(output_dir, table, snapshot_iso, sigla_uf)
    env = {
        **os.environ,
        "MALLOC_ARENA_MAX": "2",
        "MALLOC_TRIM_THRESHOLD_": "131072",
    }
    work = tempfile.mkdtemp(prefix="sicar_clean_")
    total_rows = 0
    total_dropped = 0
    peak_mb = 0.0
    try:
        parts = extract_theme_zip(zip_path, work)
        ranges = plan_shp_ranges(parts, budget_bytes=CLEAN_BUDGET_BYTES)
        for idx, (shp, start, count) in enumerate(ranges):
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pipelines.crawler.sfb_sicar._clean_runner",
                    shp,
                    str(start),
                    str(count),
                    part_dir,
                    table,
                    snapshot_iso,
                    sigla_uf,
                    str(idx),
                ],
                env=env,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"clean range {idx} ({os.path.basename(shp)} "
                    f"@{start}+{count}) failed for {sigla_uf} {table} "
                    f"(exit {proc.returncode}):\n{proc.stderr}"
                )
            result = next(
                ln
                for ln in proc.stdout.splitlines()
                if ln.startswith("RESULT ")
            )
            fields = dict(tok.split("=") for tok in result.split()[1:])
            total_rows += int(fields["rows"])
            total_dropped += int(fields["dropped"])
            peak_mb = max(peak_mb, float(fields["peak_rss_mb"]))
    finally:
        shutil.rmtree(work, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
    print(
        f"{sigla_uf} {table}: rows={total_rows} "
        f"dropped_foreign_uf={total_dropped} snapshot={snapshot_iso} "
        f"ranges={len(ranges)} peak_rss_mb_per_range={peak_mb:.0f}"
    )
    return total_rows
