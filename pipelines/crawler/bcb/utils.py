import re
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.crawler.bcb.constants import Constants
from pipelines.utils.schema_validator import validate_schema
from pipelines.utils.utils import log


def get_sicor_download_links():
    # NOTE:

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

    log("Setting up ChromeDriver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        log(f"Navigating to {url}...")
        driver.get(url)

        log("Waiting for JS to render the content...")
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
            and "DadosBrutos" in a["href"]
        ]
        log(f"Found {len(sicor_links)} .gz links on the page.")

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

    renames = config["table_schema"]

    colunas_originais = list(renames.keys())

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

        log(f"Baixando, transformando e salvando {link} em {filepath}...")

        writer = None

        chunk_iterator = pd.read_csv(
            link,
            storage_options=storage_options,
            compression="gzip",
            encoding="latin-1",
            sep=";",
            chunksize=100000,
            dtype=str,
        )

        for chunk in chunk_iterator:
            # Validate that the source columns match the expected schema
            validate_schema(chunk.columns.tolist(), colunas_originais)

            chunk = chunk.rename(columns=renames)

            table = pa.Table.from_pandas(chunk, schema=explicit_schema)

            if writer is None:
                writer = pq.ParquetWriter(filepath, explicit_schema)

            writer.write_table(table)

        if writer:
            writer.close()

        log(f"Parquet salvo com sucesso em: {filename}")

    return downloaded_paths


def create_empreendimento(id_tabela: str, download_dir: Path) -> list[Path]:
    """
    Create empreendimento table
    """
    link = "https://www.bcb.gov.br/htms/sicor/Empreendimento.csv"
    config = Constants.sicor_to_bd_table_names.value.get(id_tabela)

    renames = config["table_schema"]

    colunas_originais = list(renames.keys())

    storage_options = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    filename = link.split("/")[-1]

    filepath = Path(download_dir) / filename

    log(f"Baixando, transformando e salvando {link} em {filepath}...")

    df = pd.read_csv(
        link,
        storage_options=storage_options,
        encoding="latin-1",
        sep=";",
        dtype=str,
    )

    validate_schema(df.columns.tolist(), colunas_originais)

    df = df.rename(columns=renames)

    df.to_csv(filepath, index=False, encoding="utf-8", sep=",")

    log(f"CSV salvo com sucesso em: {filename}")

    return filepath


def parse_cobertura(row):
    """Utilizada para gerar a coluna de cobertura temporal para algumas colunas do dicionário"""
    try:
        # Formato da data nas tabelas é dd/mm/yyyy
        start_year = row["DATA_INICIO"].split("/")[-1]
        end_year = row["DATA_FIM"].split("/")[-1]
        return f"{start_year}(1){end_year}"
    except Exception as e:
        log(e)
        return None


def create_dictionary() -> pd.DataFrame:
    """
    Creates the dicionario table using the metadata defined in Constants.dicionario.

    Returns:
        pd.DataFrame: The generated dictionary DataFrame.
    """
    all_data = []
    dicionario_config = Constants.dicionario.value

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for entry in dicionario_config:
        nome_tabela = entry["nome_tabela"]
        nome_coluna = entry["nome_coluna"]
        url = entry["url"]
        colunas_map = entry["colunas"]
        sep = entry.get("sep")

        log(
            f"Processing dictionary for {nome_tabela}.{nome_coluna} from {url}"
        )

        try:
            # SICOR CSVs usam latin-1 encoding e ; ou , como sep
            # Alguns CSVs com sep="," estao com a linha inteira entre aspas, o que quebra o parse do pandas.
            # A solução encontrada foi baixar com request e corrigir as linhas problemáticas antes de passar para o pandas. Mais verboso porém funciona :)
            response = requests.get(url, headers=headers)
            content = response.content.decode("latin-1")

            lines = content.splitlines()
            fixed_lines = []
            for line in lines:
                # Marker para linhas quebradas do BCB: "1,""TR"""
                if (
                    line.startswith('"')
                    and line.endswith('"')
                    and (sep + '""') in line
                ):
                    line = line[1:-1].replace('""', '"')
                fixed_lines.append(line)

            fixed_content = "\n".join(fixed_lines)

            df = pd.read_csv(
                StringIO(fixed_content),
                sep=sep,
                dtype=str,
            )
            chave_src_col = next(
                k for k, v in colunas_map.items() if v == "chave"
            )
            valor_src_col = next(
                k for k, v in colunas_map.items() if v == "valor"
            )

            temp_df = pd.DataFrame()
            temp_df["chave"] = df[chave_src_col].str.strip()
            temp_df["valor"] = df[valor_src_col].str.strip()
            temp_df["nome_tabela"] = nome_tabela
            temp_df["nome_coluna"] = nome_coluna

            # Esses dicionários tem colunas de início e final da validade do código;
            # Um Programa, como o PRONAF, tem uma data de início e uma possível data de fim.
            if nome_coluna in [
                "id_fonte_recurso",
                "id_categoria_emitente",
                "id_programa",
            ]:
                temp_df["cobertura_temporal"] = df.apply(
                    parse_cobertura, axis=1
                )
            else:
                temp_df["cobertura_temporal"] = "(1)"

            all_data.append(temp_df)

        except Exception as e:
            log(f"Error processing dictionary entry for {nome_coluna}: {e}")
            raise

    # Concatenate all results
    final_df = pd.concat(all_data, ignore_index=True)

    # Reorder columns to the requested model
    final_df = final_df[
        ["nome_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
    ]

    # Save the output CSV
    output_dir = create_folder_structure("dicionario")
    output_path = output_dir / "dicionario.csv"
    final_df.to_csv(output_path, index=False, encoding="utf-8", sep=",")

    log(f"Dicionario CSV saved to {output_path}")

    return output_path
