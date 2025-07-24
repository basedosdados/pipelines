import ftplib
import os
import re
from datetime import datetime
from glob import glob

import pandas as pd
import py7zr
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from unidecode import unidecode

RENAME_DICT = {
    "uf": "sigla_uf",
    "municipio": "id_municipio",
    "secao": "cnae_2_secao",
    "subclasse": "cnae_2_subclasse",
    "cbo2002ocupacao": "cbo_2002",
    "saldomovimentacao": "saldo_movimentacao",
    "categoria": "categoria",
    "graudeinstrucao": "grau_instrucao",
    "idade": "idade",
    "horascontratuais": "horas_contratuais",
    "racacor": "raca_cor",
    "sexo": "sexo",
    "salario": "salario_mensal",
    "tipoempregador": "tipo_empregador",
    "tipoestabelecimento": "tipo_estabelecimento",
    "tipomovimentacao": "tipo_movimentacao",
    "tipodedeficiencia": "tipo_deficiencia",
    "indtrabintermitente": "indicador_trabalho_intermitente",
    "indtrabparcial": "indicador_trabalho_parcial",
    "tamestabjan": "tamanho_estabelecimento_janeiro",
    "indicadoraprendiz": "indicador_aprendiz",
    "origemdainformacao": "origem_informacao",
    "indicadordeforadoprazo": "indicador_fora_prazo",
    "indicadordeexclusao": "indicador_exclusao",
}


# TODO: LÓGICA DE LOOPS
def download_file(ftp, remote_dir, filename, local_dir):
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

    os.makedirs(local_dir, exist_ok=True)
    output_path = os.path.join(local_dir, filename)

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

        print(f"removendo zip corroimpido {output_path}")
        if os.path.exists(output_path):
            os.remove(output_path)

        txt_output_path = output_path.replace(".7z", ".txt")
        print(f"removendo txt corroimpido {txt_output_path}")
        if os.path.exists(txt_output_path):
            os.remove(txt_output_path)
        return False


def crawler_novo_caged_ftp(
    yearmonth: str, ftp_host: str = "ftp.mtps.gov.br", file_types: list = None
) -> list:
    """
    Downloads specified .7z files from a CAGED dataset FTP server.

    Parameters:
        yearmonth (str): the month to download data from (e.g., '202301' for January 2023)
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")
        file_types (list): list of file types to download.
                           Options: 'MOV' (movement), 'FOR' (out of deadline), 'EXC' (excluded)
                           If None, downloads all files

    Returns:
        list: List of successfully and unsuccessfully downloaded files
    """
    global CORRUPT_FILES
    CORRUPT_FILES = []

    if len(yearmonth) != 6 or not yearmonth.isdigit():
        raise ValueError("yearmonth must be a string in the format 'YYYYMM'")

    if file_types:
        file_types = [ft.upper() for ft in file_types]
        valid_types = ["MOV", "FOR", "EXC"]
        if not all(ft in valid_types for ft in file_types):
            raise ValueError(f"Invalid file types. Choose from {valid_types}")

    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"pdet/microdados/NOVO CAGED/{int(yearmonth[0:4])}/")

    available_months = ftp.nlst()
    if yearmonth not in available_months:
        raise ValueError(
            f"Month {yearmonth} is not available in the directory for the year {yearmonth[0:4]}"
        )

    ftp.cwd(yearmonth)
    print(f"Baixando para o mês: {yearmonth}")

    filenames = [f for f in ftp.nlst() if f.endswith(".7z")]

    successful_downloads = []
    failed_downloads = []

    for file in filenames:
        if "CAGEDMOV" in file and (not file_types or "MOV" in file_types):
            print(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao/input/",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif "CAGEDFOR" in file and (not file_types or "FOR" in file_types):
            print(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao_fora_prazo/input/",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif "CAGEDEXC" in file and (not file_types or "EXC" in file_types):
            print(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao_excluida/input/",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

    ftp.quit()

    print("\nDownload Summary:")
    print(f"Successfully downloaded: {successful_downloads}")
    print(f"Failed downloads: {failed_downloads}")

    if CORRUPT_FILES:
        print("\nCorrupt Files Details:")
        for corrupt_file in CORRUPT_FILES:
            print(f"Filename: {corrupt_file['filename']}")
            print(f"Local Path: {corrupt_file['local_path']}")
            print(f"Error: {corrupt_file['error']}")
            print("---")

    return {
        "successful": successful_downloads,
        "failed": failed_downloads,
        "corrupt_files": CORRUPT_FILES,
    }


def build_partitions(table_id: str, yearmonth: str) -> str:
    """
    build partitions from gtup files
    table_id: microdados_movimentacao | microdados_movimentacao_fora_prazo | microdados_movimentacao_excluida
    """
    dict_uf = {
        "11": "RO",
        "12": "AC",
        "13": "AM",
        "14": "RR",
        "15": "PA",
        "16": "AP",
        "17": "TO",
        "21": "MA",
        "22": "PI",
        "23": "CE",
        "24": "RN",
        "25": "PB",
        "26": "PE",
        "27": "AL",
        "28": "SE",
        "29": "BA",
        "31": "MG",
        "32": "ES",
        "33": "RJ",
        "35": "SP",
        "41": "PR",
        "42": "SC",
        "43": "RS",
        "50": "MS",
        "51": "MT",
        "52": "GO",
        "53": "DF",
        "99": "UF não identificada",
    }

    for uf in dict_uf.values():
        os.makedirs(
            f"/tmp/caged/{table_id}/output/ano={yearmonth[0:4]}/mes={int(yearmonth[4:])}/sigla_uf={uf}/",
            exist_ok=True,
        )

    input_files = glob(f"/tmp/caged/{table_id}/input/*{yearmonth}*")

    for filename in tqdm(input_files):
        df = pd.read_csv(filename, sep=";", dtype={"uf": str})
        date = re.search(r"\d+", filename).group()
        ano = date[:4]
        mes = int(date[-2:])
        df.columns = [unidecode(col) for col in df.columns]

        df["uf"] = df["uf"].map(dict_uf)

        df.rename(columns=RENAME_DICT, inplace=True)

        for state in dict_uf.values():
            data = df[df["sigla_uf"] == state]

            if table_id == "microdados_movimentacao":
                data = data.drop(
                    [
                        "sigla_uf",
                        "regiao",
                        "unidadesalariocodigo",
                        "valorsalariofixo",
                    ],
                    axis=1,
                )
            elif table_id == "microdados_movimentacao_fora_prazo":
                data = data.drop(
                    [
                        "sigla_uf",
                        "regiao",
                        "unidadesalariocodigo",
                        "valorsalariofixo",
                    ],
                    axis=1,
                )
            elif table_id == "microdados_movimentacao_excluida":
                data = data.drop(
                    [
                        "sigla_uf",
                        "regiao",
                        "unidadesalariocodigo",
                        "valorsalariofixo",
                    ],
                    axis=1,
                )

            data.to_csv(
                f"/tmp/caged/{table_id}/output/ano={ano}/mes={mes}/sigla_uf={state}/data.csv",
                index=False,
            )
            del data
        del df


def generate_yearmonth_range(start_date: str, end_date: str) -> list:
    """
    Generate a list of yearmonth strings between start_date and end_date (inclusive).

    Parameters:
    start_date (str): Start date in format 'YYYYMM'
    end_date (str): End date in format 'YYYYMM'

    Returns:
    list: List of yearmonth strings in chronological order

    Raises:
    ValueError: If date format is incorrect or start_date is after end_date
    """
    # Validate input format
    if not (
        len(start_date) == 6
        and len(end_date) == 6
        and start_date.isdigit()
        and end_date.isdigit()
    ):
        raise ValueError("Dates must be in 'YYYYMM' format")

    # Convert to datetime objects
    start = datetime.strptime(start_date, "%Y%m")
    end = datetime.strptime(end_date, "%Y%m")

    # Validate date order
    if start > end:
        raise ValueError("Start date must be before or equal to end date")

    # Generate list of yearmonths
    yearmonths = []
    current = start
    while current <= end:
        yearmonths.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    return yearmonths


if __name__ == "__main__":
    # NOTE: mude intervalo de datas e de tabelas conforme necessário

    date_range = generate_yearmonth_range("202504", "202505")
    table_ids = [
        "microdados_movimentacao_fora_prazo",
        "microdados_movimentacao_excluida",
        "microdados_movimentacao",
    ]

    for YEARMONTH in date_range:
        downloads = crawler_novo_caged_ftp(
            yearmonth=YEARMONTH, file_types=["EXC", "FOR", "MOV"]
        )
        print(downloads)

    for table in table_ids:
        for YEARMONTH in date_range:
            build_partitions(table_id=table, yearmonth=YEARMONTH)
