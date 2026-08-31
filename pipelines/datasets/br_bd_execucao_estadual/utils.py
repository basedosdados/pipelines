"""Download and clean helpers for br_bd_execucao_estadual.

The download and clean logic is NOT reimplemented here. It lives in
``models/br_bd_execucao_estadual/code`` and is imported from there, the same way
``br_me_siconfi`` reuses its bootstrap. That code carries a lot of hard-won detail --
Bahia's malformed-CSV repair, Pernambuco's three schema eras and two number formats,
São Paulo's cp1252 and its per-file TOTALS row, Minas Gerais' glob collision -- and a
second copy would drift from it silently. This module only decides WHAT to refresh and
where to put it.

The scratch directory is handed in per run through ``EXEC_ESTADUAL_DATA_DIR``, so a
worker never writes to ``~/Downloads`` and a retry cannot inherit a half-written file.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from pipelines.datasets.br_bd_execucao_estadual.constants import constants

CODE_DIR = constants.CODE_DIR.value


def _ensure_code_on_path(work_dir: str) -> None:
    """Point the reused code at this run's scratch dir and put it on sys.path.

    The env var has to be set BEFORE ``constants`` is first imported, because
    ``DATA_DIR`` is read at import time. Later imports get the cached module, which is
    fine: every flow run is a fresh process.
    """
    os.environ["EXEC_ESTADUAL_DATA_DIR"] = work_dir
    if CODE_DIR not in sys.path:
        sys.path.insert(0, CODE_DIR)


def input_dir(work_dir: str, state: str) -> Path:
    return Path(work_dir) / "input" / state.lower()


def output_dir(work_dir: str, table: str) -> Path:
    return Path(work_dir) / "output" / table


def stale_current_year(work_dir: str, state: str, year: int) -> int:
    """Delete the open exercise's raw files so the downloader re-fetches them.

    The downloaders skip any file already on disk, which is what makes them resumable.
    For a recurring refresh that is the wrong behaviour for the CURRENT exercise: it is
    the only one the state still rewrites. Removing it is what turns "resume" into
    "refresh", and it is confined to the open year so the other twenty-odd are not
    re-downloaded daily.

    Bahia and São Paulo are not handled here -- BA ships whole-dataset ZIPs with no
    per-year file to delete, and SP is re-scraped per (exercise, órgão) by the caller.
    """
    directory = input_dir(work_dir, state)
    if not directory.exists():
        return 0
    removed = 0
    for path in directory.iterdir():
        if path.is_file() and str(year) in path.name:
            path.unlink()
            removed += 1
    return removed


def refresh_mg(work_dir: str, year: int, full: bool) -> None:
    _ensure_code_on_path(work_dir)
    # pyrefly: ignore [missing-import]
    import clean_mg

    # pyrefly: ignore [missing-import]
    import download_mg

    if not full:
        n = stale_current_year(work_dir, "mg", year)
        print(f"MG: dropped {n} file(s) for {year} so they refresh")
    download_mg.main()
    clean_mg.main()


def refresh_ba(work_dir: str, year: int, full: bool) -> None:
    """Bahia always refreshes whole.

    The state publishes one ZIP per view covering every exercise, so there is no
    per-year file to invalidate: a refresh is a re-download of all six views. They are
    the smallest of the four states, so this is cheap enough to run daily.
    """
    _ensure_code_on_path(work_dir)
    # pyrefly: ignore [missing-import]
    import clean_ba

    # pyrefly: ignore [missing-import]
    import download_ba

    directory = input_dir(work_dir, "ba")
    if directory.exists():
        shutil.rmtree(directory)
    download_ba.main()
    clean_ba.main()


def refresh_pe(work_dir: str, year: int, full: bool) -> None:
    _ensure_code_on_path(work_dir)
    # pyrefly: ignore [missing-import]
    import clean_pe

    # pyrefly: ignore [missing-import]
    import download_pe

    if not full:
        n = stale_current_year(work_dir, "pe", year)
        print(f"PE: dropped {n} file(s) for {year} so they refresh")
    download_pe.main()
    # Both kinds, because `despesa` and `pagamento` are separate CKAN packages that
    # both republish the open exercise.
    clean_pe.main()


def refresh_sp(work_dir: str, year: int, full: bool) -> None:
    """São Paulo, scraped one (exercise, órgão) at a time.

    A full pass is about 540 queries at ~36 s each -- five hours -- so a refresh scrapes
    only the open exercise, roughly 32 queries and twenty minutes. `clean_sp` then
    rebuilds every year's parquet from whatever is on disk, which is why the incremental
    path still needs the earlier exercises present.
    """
    _ensure_code_on_path(work_dir)
    # pyrefly: ignore [missing-import]
    import clean_sp

    # pyrefly: ignore [missing-import]
    import download_sp

    if full:
        download_sp.main()
    else:
        stale_current_year(work_dir, "sp", year)
        download_sp.main(first_year=year, last_year=year)
    clean_sp.main()


REFRESHERS = {
    "MG": refresh_mg,
    "BA": refresh_ba,
    "PE": refresh_pe,
    "SP": refresh_sp,
}


def built_tables(work_dir: str, state: str) -> dict[str, Path]:
    """The staging mirrors this state produced, mapped to their parquet directory.

    Only directories that actually contain parquet are returned: a source that
    published nothing new leaves an empty directory, and uploading that would replace a
    populated prefix in the bucket with nothing.
    """
    wanted = constants.STAGING_BY_STATE.value[state]
    out: dict[str, Path] = {}
    for table in wanted:
        directory = output_dir(work_dir, table)
        if directory.is_dir() and any(directory.glob("*.parquet")):
            out[table] = directory
    return out
