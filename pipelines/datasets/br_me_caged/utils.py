import ftplib
from pathlib import Path
from typing import List

import py7zr

from pipelines.utils.utils import log


def download_file(
    ftp: ftplib.FTP, remote_dir: str, filename: str, local_dir: str | Path
) -> List:
    """
    Downloads and extracts a .7z file from an FTP server with error handling.

    Parameters:
        ftp (ftplib.FTP): an active FTP connection
        remote_dir (str): the remote directory containing the file
        filename (str): the name of the file to download
        local_dir (str): the local directory to save and extract the file

    Returns:
        bool: True if file downloaded and extracted successfully, False otherwise
    """
    CORRUPT_FILE = []
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    output_path = local_dir / filename

    try:
        with open(output_path, "wb") as f:
            ftp.retrbinary("RETR " + filename, f.write)

        try:
            with py7zr.SevenZipFile(output_path, "r") as archive:
                archive.extractall(path=local_dir)

            output_path.unlink()
            return True, CORRUPT_FILE

        except py7zr.Bad7zFile as extract_error:
            log(f"Error extracting file {filename}: {extract_error}")
            CORRUPT_FILE = {
                "filename": filename,
                "local_path": output_path,
                "error": str(extract_error),
            }
            return False, CORRUPT_FILE

    except Exception as download_error:
        log(f"Error downloading file {filename}: {download_error}")
        CORRUPT_FILE = {
            "filename": filename,
            "local_path": output_path,
            "error": str(download_error),
        }

        log(f"removendo zip corrompido {output_path}")
        if output_path.exists():
            output_path.unlink()

        txt_output_path = Path(str(output_path).replace(".7z", ".txt"))
        log(f"removendo txt corrompido {txt_output_path}")
        if txt_output_path.exists():
            txt_output_path.unlink()
        return False, CORRUPT_FILE


def verify_yearmonth(yearmonth: str):
    if len(yearmonth) != 6 or not yearmonth.isdigit():
        raise ValueError("yearmonth must be a string in the format 'YYYYMM'")
