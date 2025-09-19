import ftplib
import os
from pathlib import Path

import py7zr


def download_file(
    ftp: ftplib.FTP, remote_dir: str, filename: str, local_dir: str | Path
):
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
    global CORRUPT_FILES
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    output_path = local_dir / filename

    try:
        with open(output_path, "wb") as f:
            ftp.retrbinary("RETR " + filename, f.write)

        try:
            with py7zr.SevenZipFile(output_path, "r") as archive:
                archive.extractall(path=local_dir)

            os.remove(output_path)
            return True

        except py7zr.Bad7zFile as extract_error:
            print(f"Error extracting file {filename}: {extract_error}")
            CORRUPT_FILES.append(
                {
                    "filename": filename,
                    "local_path": output_path,
                    "error": str(extract_error),
                }
            )

            return False

    except Exception as download_error:
        print(f"Error downloading file {filename}: {download_error}")
        CORRUPT_FILES.append(
            {
                "filename": filename,
                "local_path": output_path,
                "error": str(download_error),
            }
        )

        print(f"removendo zip corrompido {output_path}")
        if os.path.exists(output_path):
            os.remove(output_path)

        txt_output_path = output_path.replace(".7z", ".txt")
        print(f"removendo txt corrompido {txt_output_path}")
        if os.path.exists(txt_output_path):
            os.remove(txt_output_path)
        return False


def verify_yearmonth(yearmonth: str):
    if len(yearmonth) != 6 or not yearmonth.isdigit():
        raise ValueError("yearmonth must be a string in the format 'YYYYMM'")
