import re
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.datasets.br_bcb_sicor.constants import Constants
from pipelines.utils.utils import log


def get_sicor_download_links():

    url = Constants.URL.value

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Adding a standard user-agent helps bypass basic bot checks
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    print("Setting up ChromeDriver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        print(f"Navigating to {url}...")
        driver.get(url)

        print("Waiting for JS to render the content...")
        WebDriverWait(driver, 8).until(
            ec.presence_of_element_located((By.TAG_NAME, "a"))
        )

        time.sleep(2)

        html_content = driver.page_source

        soup = BeautifulSoup(html_content, "html.parser")

        # recor é o sistema anterior ao sicor, por isso é ignorado. A pipeline extrai dados do sicor somente;
        sicor_links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if (".gz" in a["href"] or ".csv" in a["href"])
            and "recor" not in a["href"]
            # and "DadosBrutos" in a["href"]
        ]
        print(f"Found {len(sicor_links)} .gz links on the page.")

        return sicor_links

    except Exception as e:
        log(f"An error occurred:{e}")
        raise

    finally:
        driver.quit()


def build_sicor_download_df(links: list) -> pd.DataFrame:
    """
    Creates a DataFrame with download link information, mapping raw names to standardized table IDs.

    Args:
        links (list): List of download links.

    Returns:
        pd.DataFrame: DataFrame with columns [id_tabela, link, tipo_liberacao_arquivo, ano, mes]
    """
    data = []
    mapping = Constants.sicor_to_bd_table_names.value

    for link in links:
        filename = link.split("/")[-1]
        name_no_ext = re.sub(r"\.(gz|csv)$", "", filename, flags=re.IGNORECASE)
        clean_name = re.sub(r"\d+", "", name_no_ext).strip("_")

        id_tabela = None
        for table_id, info in mapping.items():
            raw_name = info["table_raw_name"]
            if clean_name.lower().endswith(raw_name.lower()):
                id_tabela = table_id
                break

        if id_tabela:
            year_match = re.search(r"_(\d{4})", link)
            ano = year_match.group(1) if year_match else None

            tipo_liberacao_arquivo = "yearly" if ano else "unique"

            indice_arquivo = None
            if ano:
                period_match = re.search(rf"_{ano}_(\d{{2}})", link)
                if period_match:
                    indice_arquivo = period_match.group(1)

            data.append(
                {
                    "id_tabela": id_tabela,
                    "link": link,
                    "tipo_liberacao_arquivo": tipo_liberacao_arquivo,
                    "ano": ano,
                    "mindice_arquivos": indice_arquivo,
                }
            )

    return pd.DataFrame(data)


def create_folder_structure(id_tabela: str) -> Path:

    path = Path().cwd()
    output_path = path / Constants.OUTPUT_FOLDER.value / id_tabela

    output_path.mkdir(parents=True, exist_ok=True)

    return output_path


def download_standardize(
    data: pd.DataFrame, id_tabela: str, download_dir: Path
) -> list[Path]:

    data = data[data["id_tabela"] == id_tabela]

    config = Constants.sicor_to_bd_table_names.value.get(id_tabela)

    if not config:
        raise ValueError(
            f"Configurações para a tabela '{id_tabela}' não encontradas em Constants."
        )

    renames = config["table_schema"]

    colunas_originais = list(renames.keys())
    colunas_finais_ordenadas = list(renames.values())

    pyarrow_fields = []

    for _col_original, col_final in renames.items():
        tipo_pa = pa.string()
        pyarrow_fields.append(pa.field(str(col_final), tipo_pa))

    explicit_schema = pa.schema(pyarrow_fields)

    storage_options = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    downloaded_paths = []

    for _, row in data.iterrows():
        link = row["link"]
        filename = (
            link.split("/")[-1]
            .replace(".csv.gz", ".parquet")
            .replace(".gz", ".parquet")
        )
        filepath = Path(download_dir) / filename
        downloaded_paths.append(filepath)

        print(f"Baixando, transformando e salvando {link} em {filepath}...")

        writer = None

        chunk_iterator = pd.read_csv(
            link,
            storage_options=storage_options,
            compression="gzip",
            encoding="latin-1",
            sep=";",
            chunksize=100000,
            usecols=lambda col: col in colunas_originais,
            dtype=str,
        )

        for chunk in chunk_iterator:
            chunk = chunk.rename(columns=renames)

            for col in colunas_finais_ordenadas:
                if col not in chunk.columns:
                    chunk[col] = None

            chunk = chunk[colunas_finais_ordenadas]

            table = pa.Table.from_pandas(chunk, schema=explicit_schema)

            if writer is None:
                writer = pq.ParquetWriter(filepath, explicit_schema)

            writer.write_table(table)

        if writer:
            writer.close()

        print(f"Parquet salvo com sucesso em: {filename}")

    return downloaded_paths
