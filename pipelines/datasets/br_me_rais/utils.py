"""
Utilities for br_me_rais
"""

import ftplib
from pathlib import Path

import py7zr

from pipelines.datasets.br_me_rais.constants import constants as rais_constants
from pipelines.utils.utils import log


def _progress_callback(f, filename: str, offset: int, total: int | None):
    """Return a retrbinary callback that writes chunks and logs every FTP_LOG_INTERVAL bytes."""
    interval = rais_constants.FTP_LOG_INTERVAL.value
    downloaded = [offset]
    since_last = [0]

    def _cb(chunk: bytes) -> None:
        f.write(chunk)
        downloaded[0] += len(chunk)
        since_last[0] += len(chunk)
        if since_last[0] >= interval:
            since_last[0] = 0
            mb = downloaded[0] / 1_048_576
            if total:
                pct = downloaded[0] / total * 100
                log(
                    f"{filename}: {mb:.0f} MB / {total / 1_048_576:.0f} MB ({pct:.1f}%)"
                )
            else:
                log(f"{filename}: {mb:.0f} MB downloaded")

    return _cb


def download_rais_file(
    ftp: ftplib.FTP,
    filename: str,
    local_dir: Path,
    blocksize: int = 1_048_576,
) -> tuple[bool, dict | list]:
    """Download and extract a single .7z file from the RAIS FTP.

    Assumes ftp is already cwd'd to the correct year directory.
    Uses REST STREAM to resume partial downloads when the server supports it.
    Returns (success, error_info). On failure preserves any partial file for the next attempt.
    """
    local_7z = local_dir / filename
    extracted_name = filename.replace(".7z", "")

    offset = local_7z.stat().st_size if local_7z.exists() else 0
    mode = "ab" if offset else "wb"

    try:
        total = ftp.size(filename)
    except Exception:
        total = None

    total_mb = f"{total / 1_048_576:.0f} MB" if total else "unknown size"
    if offset:
        log(
            f"Resuming {filename} from {offset / 1_048_576:.0f} MB ({total_mb})"
        )
    else:
        log(f"Starting {filename} ({total_mb})")

    try:
        with open(local_7z, mode) as f:
            ftp.retrbinary(
                f"RETR {filename}",
                _progress_callback(f, filename, offset, total),
                blocksize=blocksize,
                rest=offset or None,
            )
    except Exception as e:
        log(f"Download failed for {filename} at offset {offset:,}: {e}")
        return False, {"file": filename, "error": str(e)}

    try:
        with py7zr.SevenZipFile(local_7z, mode="r") as archive:
            archive.extractall(path=local_dir)
        local_7z.unlink(missing_ok=True)
        log(f"Downloaded and extracted {filename}")
        return True, []
    except py7zr.exceptions.Bad7zFile as e:
        log(f"Corrupt archive {filename}: {e}")
        local_7z.unlink(missing_ok=True)
        (local_dir / extracted_name).unlink(missing_ok=True)
        return False, {"file": filename, "error": str(e)}
