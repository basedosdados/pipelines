"""
General purpose functions for the br_cvm_fi project
"""

import csv
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from unidecode import unidecode

from pipelines.crawler.cvm.constants import constants as cvm_constants
from pipelines.utils.utils import log, to_partitions

# Define configuration for each table type
TABLE_CONFIGS = {
    "documentos_informe_diario": {
        "url": cvm_constants.URL_INFORME_DIARIO.value,
        "files": [],
        "rename_mapping": cvm_constants.RENAME_MAPPING_INFORME.value,
        "final_columns": cvm_constants.FINAL_COLS_INFORME.value,
        "cnpj_columns": ["cnpj"],
        "create_partition_columns": True,
    },
    "documentos_carteiras_fundos_investimento": {
        "url": cvm_constants.URL_CDA,
        "files": [],
        "rename_mapping": cvm_constants.RENAME_MAPPING_CDA.value,
        "final_columns": cvm_constants.FINAL_COLS_CDA.value,
        "to_map_columns": cvm_constants.TO_MAP_COLS_CDA.value,
        "mapping": cvm_constants.MAPEAMENTO.value,
        "cnpj_columns": [
            "cnpj",
            "cnpj_instituicao_financeira_coobrigacao",
            "indicador_codigo_identificacao_emissor_pessoa_fisica_juridica",
            "cnpj_emissor",
            "cnpj_fundo_investido",
        ],
        "ascii_columns": cvm_constants.ASCII_COLS_CDA.value,
        "create_partition_columns": True,
    },
    "documentos_extratos_informacoes": {
        "url": cvm_constants.URL_EXTRATO.value,
        "files": ["extrato_fi.csv"],
        "rename_mapping": cvm_constants.RENAME_MAPPING_EXTRATO.value,
        "final_columns": cvm_constants.FINAL_COLS_EXTRATO.value,
        "to_map_columns": cvm_constants.TO_MAP_COLS_EXTRATO.value,
        "mapping": cvm_constants.MAPEAMENTO.value,
        "cnpj_columns": ["cnpj"],
        "ascii_columns": cvm_constants.ASCII_COLS_EXTRATO.value,
        "create_partition_columns": True,
    },
    "documentos_perfil_mensal": {
        "url": cvm_constants.URL_PERFIL_MENSAL.value,
        "files": [],
        "rename_mapping": cvm_constants.RENAME_MAPPING_PERFIL.value,
        "final_columns": cvm_constants.FINAL_COLS_PERFIL.value,
        "to_map_columns": cvm_constants.TO_MAP_COLS_PERFIL.value,
        "mapping": cvm_constants.MAPEAMENTO.value,
        "cnpj_columns": [
            "cnpj",
            "cpf_cnpj_comitente_1",
            "cpf_cnpj_comitente_2",
            "cpf_cnpj_comitente_3",
            "cpf_cnpj_emissor_1",
            "cpf_cnpj_emissor_2",
            "cpf_cnpj_emissor_3",
        ],
        "ascii_columns": cvm_constants.ASCII_COLS_PERFIL_MENSAL.value,
        "create_partition_columns": True,
    },
    "documentos_informacao_cadastral": {
        "url": cvm_constants.URL_INFO_CADASTRAL.value,
        "files": ["cad_fi.csv"],
        "rename_mapping": cvm_constants.RENAME_MAPPING_CAD.value,
        "final_columns": cvm_constants.FINAL_COLS_CAD.value,
        "cnpj_columns": [
            "cnpj",
            "cnpj_administrador",
            "cpf_cnpj_gestor",
            "cnpj_auditor",
            "cnpj_custodiante",
            "cnpj_controlador",
        ],
        "ascii_columns": cvm_constants.ASCII_COLS_CAD.value,
        "create_partition_columns": False,  # CAD doesn't use partitions
    },
    "documentos_balancete": {
        "url": cvm_constants.URL_BALANCETE.value,
        "files": [],
        "rename_mapping": cvm_constants.RENAME_MAPPING_BALANCETE.value,
        "final_columns": cvm_constants.FINAL_COLS_BALANCETE.value,
        "cnpj_columns": ["cnpj"],
        "create_partition_columns": True,
    },
}


def process_file(config, file_path: Path) -> pd.DataFrame:
    """Read and basic process a CSV file."""
    log(f"Processing file: {file_path}")

    df = pd.read_csv(
        file_path,
        sep=";",
        encoding="ISO-8859-1",
        dtype="string",
        quoting=csv.QUOTE_NONE,
    )

    # Apply rename mapping
    if "rename_mapping" in config:
        df = df.rename(columns=config["rename_mapping"])

    # Create year/month columns if needed
    if config.get("create_partition_columns", True):
        df = create_year_month_columns(df)

    return df


def apply_common_transformations(
    config: dict, df: pd.DataFrame
) -> pd.DataFrame:
    """Apply common transformations to the dataframe."""
    # Ensure final columns
    if "final_columns" in config:
        df = check_and_create_column(df, config["final_columns"])

    # Apply value mapping
    if "to_map_columns" in config and "mapping" in config:
        df[config["to_map_columns"]] = df[config["to_map_columns"]].map(
            lambda x: config["mapping"].get(x, x)
        )

    # Format CNPJ columns
    if "cnpj_columns" in config:
        df = format_cnpj_columns(df, config["cnpj_columns"])

    # Replace commas with dots in numeric columns
    df = df.replace(",", ".", regex=True)

    # Clean ASCII columns
    if "ascii_columns" in config:
        df[config["ascii_columns"]] = df[config["ascii_columns"]].fillna("")
        df[config["ascii_columns"]] = df[config["ascii_columns"]].map(
            limpar_string
        )

    # Select final columns
    if "final_columns" in config:
        df = df[config["final_columns"]]

    return df


def save_output(
    config: str, df: pd.DataFrame, table_id: str, use_partitions: bool = True
) -> str:
    """Save the processed data to output directory."""
    output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if use_partitions:
        to_partitions(
            df,
            partition_columns=["ano", "mes"],
            savepath=output_dir,
        )
    else:
        df.to_csv(
            output_dir / "data.csv",
            encoding="utf-8",
            index=False,
        )
        return str(output_dir / "data.csv")

    return str(output_dir)


def obter_anos_meses(diretorio: str | Path):
    """
    Retorna uma lista com todos os arquivos AAAAMM presentes no diretório.
    """

    lista_arquivos = Path(diretorio).iterdir()
    padrao_aaaamm = re.compile(r"cda_fi_BLC_\d+_(\d{6}).csv")

    anos_meses = set()
    for arquivo in lista_arquivos:
        match = padrao_aaaamm.match(arquivo.name)
        if match:
            ano_mes = match.group(1)
            anos_meses.add(ano_mes)

    return list(anos_meses)


def limpar_string(texto):
    texto = str(texto)
    # Remover acentos
    texto = unidecode(texto)
    # Remover pontuações
    texto = re.sub(r"[^\w\s]", "", texto)
    # Converter para letras minúsculas
    texto = texto.lower()
    return texto


def check_and_create_column(
    df: pd.DataFrame, colunas_totais: list
) -> pd.DataFrame:
    """
    Check if a column exists in a Pandas DataFrame. If it doesn't, create a new column with the given name
    and fill it with NaN values. If it does exist, do nothing.

    Parameters:
    df (Pandas DataFrame): The DataFrame to check.
    col_name (str): The name of the column to check for or create.

    Returns:
    Pandas DataFrame: The modified DataFrame.
    """
    for col_name in colunas_totais:
        if col_name not in df.columns:
            df[col_name] = ""
    return df


def create_year_month_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["ano"] = df["data_competencia"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d").year
    )
    df["mes"] = df["data_competencia"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d").month
    )
    return df


def format_cnpj_columns(
    df: pd.DataFrame, cnpj_columns: list[str]
) -> pd.DataFrame:
    for col in cnpj_columns:
        df[col] = df[col].str.replace(r"[/.-]", "")
    return df
