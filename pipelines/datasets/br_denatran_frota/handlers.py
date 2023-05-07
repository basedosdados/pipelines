# -*- coding: utf-8 -*-
"""
Tasks for br_denatran_frota
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

from prefect import task
import os
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.utils import (
    make_dir_when_not_exists,
    extract_links_post_2012,
    verify_total,
    change_df_header,
    guess_header,
    get_year_month_from_filename,
    call_downloader,
    generic_extractor,
)
import pandas as pd
import polars as pl

MONTHS = constants.MONTHS.value
DATASET = constants.DATASET.value
DICT_UFS = constants.DICT_UFS.value
OUTPUT_PATH = constants.OUTPUT_PATH.value


def crawl(month: int, year: int, temp_dir: str = ""):
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")
    files_dir = os.path.join(temp_dir, "files")
    make_dir_when_not_exists(files_dir)
    year_dir_name = os.path.join(files_dir, f"{year}")
    make_dir_when_not_exists(year_dir_name)
    if year > 2012:
        files_to_download = extract_links_post_2012(month, year, year_dir_name)
        for file_dict in files_to_download:
            call_downloader(file_dict)
    else:
        url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"
        info = {
            "txt": "Dados anuais",
            "href": url,
            "mes_name": MONTHS.get(month),
            "mes": month,
            "ano": year,
            "filetype": "zip",
            "destination_dir": temp_dir,
        }
        call_downloader(info)


def treat_uf_tipo(file) -> pl.DataFrame:
    filename = os.path.split(file)[1]
    df = pd.read_excel(file)
    new_df = change_df_header(df, guess_header(df))
    # This is ad hoc for UF_tipo.
    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf"}, inplace=True
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    clean_df = new_df[new_df.sigla_uf.isin(DICT_UFS.values())].reset_index(
        drop=True
    )  # Now we get all the actual RELEVANT uf data.
    month, year = get_year_month_from_filename(filename)
    clean_pl_df = pl.from_pandas(clean_df).lazy()
    verify_total(clean_pl_df.collect())
    # Add year and month
    clean_pl_df = clean_pl_df.with_columns(
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    clean_pl_df = clean_pl_df.select(pl.exclude("TOTAL"))
    clean_pl_df = clean_pl_df.melt(
        id_vars=["ano", "mes", "sigla_uf"],
        variable_name="tipo_veiculo",
        value_name="quantidade",
    )  # Long format.
    clean_pl_df = clean_pl_df.collect()
    return clean_pl_df


def output_file_to_csv(df: pl.DataFrame) -> None:
    pass


# df.write_csv(file=f"{OUTPUT_PATH}/{filename}.csv", has_header=True)
