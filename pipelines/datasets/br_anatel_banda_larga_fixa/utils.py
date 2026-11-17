from os import makedirs
from os.path import join

import pandas as pd
import polars as pl

from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log, to_partitions


def treatment(csv_path: str, table_id: str) -> None:
    """Trata o CSV do microdados e grava uma partição por ano, mês e UF.

    Cada partição vai para `ano=<ano>/mes=<mes>/sigla_uf=<uf>/data.csv`, sem as
    colunas de partição. O CSV é lido como texto e tratado um mês por vez.

    Args:
        csv_path: Caminho do `Acessos_Banda_Larga_Fixa_<ano>.csv`.
        table_id: Tabela que recebe as partições.
    """

    log("Iniciando o tratamento do arquivo microdados da Anatel")

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    if "Tipo de Produto" not in df.columns:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("Tipo de Produto")
        )

    cleaned_df = (
        df.rename(anatel_constants.RENAME_MICRODADOS.value)
        .select(anatel_constants.ORDER_COLUMNS_MICRODADOS.value)
        .with_columns(
            pl.col("transmissao").str.replace_many(
                [
                    "Cabo Metálico",
                    "Satélite",
                    "Híbrido",
                    "Fibra Óptica",
                    "Rádio",
                ],
                [
                    "Cabo Metalico",
                    "Satelite",
                    "Hibrido",
                    "Fibra Optica",
                    "Radio",
                ],
            ),
            pl.col("produto")
            .str.replace("LINHA_DEDICADA", "linha dedicada", literal=True)
            .str.to_lowercase(),
            pl.col("ano", "mes", "sigla_uf").fill_null(""),
        )
    )

    one_month_per_row_table = (
        cleaned_df.select("ano", "mes").unique().collect()
    )

    log("Salvando o arquivo microdados da Anatel")

    for year, month in one_month_per_row_table.iter_rows():
        month_df = cleaned_df.filter(
            (pl.col("ano") == year) & (pl.col("mes") == month)
        ).collect()

        for uf_df in month_df.partition_by("sigla_uf"):
            uf = uf_df["sigla_uf"][0]

            path = join(
                anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
                f"ano={year}",
                f"mes={month}",
                f"sigla_uf={uf}",
            )

            makedirs(path, exist_ok=True)

            uf_df.drop("ano", "mes", "sigla_uf").write_csv(
                join(path, "data.csv")
            )


def _parse_densidade(x: str | float | None) -> float | None:
    """Converte a densidade da fonte, com vírgula decimal, para float.

    Args:
        x: Valor da célula: texto como `"12,34"`, ou NaN/None quando a
            densidade está vazia.

    Returns:
        A densidade como float, ou o próprio valor quando a célula está vazia.
    """

    if isinstance(x, str):
        return float(x.replace(",", "."))

    return x


def treatment_br(csv_path: str, table_id: str) -> None:
    """Trata o CSV de densidade e grava as linhas do nível Brasil.

    Grava `densidade_brasil.csv` na pasta de saída da tabela.

    Args:
        csv_path: Caminho do `Densidade_Banda_Larga_Fixa.csv`.
        table_id: Tabela que recebe o arquivo.
    """

    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(
        anatel_constants.DROP_COLUMNS_BRASIL.value, axis=1
    )
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(_parse_densidade)
    df_brasil = df_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value,
    )
    log("Salvando o arquivo densidade brasil da Anatel")
    df_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_uf(csv_path: str, table_id: str) -> None:
    """Trata o CSV de densidade e grava as linhas do nível UF.

    Grava `densidade_uf.csv` na pasta de saída da tabela.

    Args:
        csv_path: Caminho do `Densidade_Banda_Larga_Fixa.csv`.
        table_id: Tabela que recebe o arquivo.
    """

    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_uf = df[df["Geografia"] == "UF"]
    df_uf = df_uf.drop(anatel_constants.DROP_COLUMNS_UF.value, axis=1)
    df_uf["Densidade"] = df_uf["Densidade"].apply(_parse_densidade)
    df_uf = df_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value,
    )
    log("Iniciando o particionado do arquivo densidade uf da Anatel")
    df_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_municipio(csv_path: str, table_id: str) -> None:
    """Trata o CSV de densidade e grava as linhas do nível município.

    Descarta as linhas sem `Código IBGE` e grava uma partição por ano, em
    `ano=<ano>/data.csv`, sem a coluna de partição.

    Args:
        csv_path: Caminho do `Densidade_Banda_Larga_Fixa.csv`.
        table_id: Tabela que recebe as partições.
    """

    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio = df_municipio.drop(["Município", "Geografia"], axis=1)
    df_municipio = df_municipio.dropna(subset=["Código IBGE"])
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        _parse_densidade
    )
    df_municipio = df_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value,
    )
    log("Salvando o arquivo densidade municipio da Anatel")
    to_partitions(
        df_municipio,
        partition_columns=["ano"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )
