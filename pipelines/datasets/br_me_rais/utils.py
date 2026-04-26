"""
Utilities for br_me_rais
"""

import ftplib
from pathlib import Path

import py7zr

from pipelines.utils.utils import log


def download_rais_file(
    ftp: ftplib.FTP,
    filename: str,
    local_dir: Path,
) -> tuple[bool, dict | list]:
    """Download and extract a single .7z file from the RAIS FTP.

    Assumes ftp is already cwd'd to the correct year directory.
    Returns (success, error_info). On failure cleans up any partial files.
    """
    local_7z = local_dir / filename
    extracted_name = filename.replace(".7z", "")

    try:
        with open(local_7z, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
    except Exception as e:
        log(f"Download failed for {filename}: {e}")
        local_7z.unlink(missing_ok=True)
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
