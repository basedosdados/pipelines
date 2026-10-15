import ftplib
import re
from pathlib import Path

import pandas as pd
import py7zr

from pipelines.utils.utils import log


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
    corrupt_file = []
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
            return True, corrupt_file

        except py7zr.Bad7zFile as extract_error:
            log(f"Error extracting file {filename}: {extract_error}")
            corrupt_file = {
                "filename": filename,
                "local_path": output_path,
                "error": str(extract_error),
            }
            return False, corrupt_file

    except Exception as download_error:
        log(f"Error downloading file {filename}: {download_error}")
        corrupt_file = {
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
        return False, corrupt_file


def verify_year_month(year_month: str):
    if len(year_month) != 6 or not year_month.isdigit():
        raise ValueError("year_month must be a string in the format 'YYYYMM'")


def count_data_rows(path: str | Path) -> int:
    """Conta as linhas não vazias de um arquivo, sem o cabeçalho.

    Args:
        path: Caminho do arquivo.

    Returns:
        O número de linhas de dado.
    """
    total_rows = 0

    with open(path, mode="rb") as f:
        for row in f:
            if row.strip() != b"":
                total_rows += 1

    return total_rows - 1


def year_month_from_filename(path: str | Path) -> str:
    """Extrai o mês do nome de um arquivo do CAGED.

    Args:
        path: Caminho do arquivo, como `CAGEDMOV202608.txt`.

    Returns:
        O mês no formato `AAAAMM`.
    """
    # pyrefly: ignore [missing-attribute]
    return re.search(r"\d+", Path(path).name).group()


def month_partition_paths(
    table_output_dir: str | Path, year_month: str
) -> list[Path]:
    """Lista os `data.csv` das partições de UF de um mês.

    Args:
        table_output_dir: Pasta com as partições `ano=/mes=/sigla_uf=/data.csv`.
        year_month: Mês no formato `AAAAMM`.

    Returns:
        Os caminhos dos `data.csv` do mês.
    """
    year = year_month[:4]
    month = int(year_month[-2:])
    return list(
        Path(table_output_dir).glob(
            f"ano={year}/mes={month}/sigla_uf=*/data.csv"
        )
    )


def count_competence_violations(
    competences: pd.DataFrame, table_id: str, year_month: str
) -> int:
    """Conta as linhas cuja competência quebra a regra da tabela.

    Args:
        competences: Colunas de competência de uma partição.
        table_id: Tabela do conjunto.
        year_month: Mês do arquivo, no formato `AAAAMM`.

    Returns:
        O número de linhas fora da regra, contando competência nula.

    Raises:
        ValueError: Se a tabela não tiver regra de competência.
    """
    competence = competences["competenciamov"].str[:6]

    if table_id == "microdados_movimentacao":
        valid = competence == year_month
    elif table_id == "microdados_movimentacao_fora_prazo":
        valid = competence < year_month
    elif table_id == "microdados_movimentacao_excluida":
        declaration = competences["competenciadec"].str[:6]
        valid = (competence <= year_month) & (declaration <= year_month)
    else:
        raise ValueError(f"Tabela sem regra de competência: {table_id}")

    return (~valid).sum()
