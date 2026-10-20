"""Tasks for br_cvm_administradores_carteira — Prefect 3.

`crawl`/`clean_table_*` fazem o download e a limpeza das três tabelas do
dataset — migrados de `pipelines/shared/cvm_administradores_carteira/`
(pasta removida; este módulo é o único consumidor real daquele código desde
a migração pro pipeline em estágios).

Check diferente do padrão usual: a fonte (URL estática única da CVM, sem
listagem de diretório nem competência no nome do arquivo) não expõe nenhuma
data pra comparar. O sinal usado aqui é a **contagem de linhas**: baixa o
ZIP, limpa a tabela, compara contra o que já está publicado no BigQuery —
mais linhas na fonte do que em prod significa dado novo.

Quando há mais linhas, `reference_date` vira a data de hoje. Quando não há,
`reference_date` recebe um valor fixo no passado, sempre mais antigo que
qualquer materialização real: `compare_against="table_update"` compara
contra `Table.Update.latest`, que é `bq.last_modified()` (metadado do
próprio BigQuery, atualizado sozinho toda vez que a tabela é reescrita de
verdade) — não algo que este check escreve diretamente. Por isso o valor
sentinela nunca gera falso positivo, mesmo em runs consecutivos sem dado
novo.
"""

import datetime
import os
import shutil
from collections.abc import Callable

import pandas as pd
import requests
from google.cloud import bigquery
from pandas.api.types import is_string_dtype
from prefect import task
from unidecode import unidecode

from pipelines.datasets.br_cvm_administradores_carteira.constants import (
    DATASET_ID,
    PESSOA_FISICA_COVERAGE,
    PESSOA_FISICA_TABLE_ID,
    PESSOA_JURIDICA_COVERAGE,
    PESSOA_JURIDICA_TABLE_ID,
    RESPONSAVEL_COVERAGE,
    RESPONSAVEL_TABLE_ID,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

_BQ_PROJECT = "basedosdados"

# Sentinela sempre mais antigo que qualquer `Table.Update.latest` real.
_NO_NEW_DATA_DATE = datetime.date(2000, 1, 1)

_CRAWL_ROOT = "/tmp/data_cvm_adm_cart"
_CRAWL_URL = (
    "http://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip"
)


@task(retries=2, retry_delay_seconds=30)
def crawl(
    root: str = _CRAWL_ROOT, url: str = _CRAWL_URL, chunk_size: int = 128
) -> None:
    """Download and unzip dataset br_cvm_administradores_carteira."""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)
    os.makedirs(f"{root}/cleaned", exist_ok=True)

    response = requests.get(url, timeout=300)

    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            file.write(chunk)

    shutil.unpack_archive(filepath, extract_dir=root)


@task
def clean_table_responsavel(root: str = _CRAWL_ROOT) -> str:
    in_filepath = f"{root}/cad_adm_cart_resp.csv"
    ou_filepath = f"{root}/cleaned/bd_responsavel.csv"

    dataframe = pd.read_csv(
        in_filepath,
        sep=";",
        na_values="",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    dataframe.columns = ["cnpj", "nome", "tipo"]
    dataframe["cnpj"] = (
        dataframe["cnpj"]
        .str.replace(".", "", regex=False)
        .str.replace("/", "", regex=False)
        .str.replace("-", "", regex=False)
    )

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False, encoding="utf-8")
    return ou_filepath


@task
def clean_table_pessoa_fisica(root: str = _CRAWL_ROOT) -> str:
    in_filepath = f"{root}/cad_adm_cart_pf.csv"
    ou_filepath = f"{root}/cleaned/bd_pessoa_fisica.csv"

    dataframe = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    dataframe.columns = [
        "nome",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
    ]

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False)
    return ou_filepath


@task
def clean_table_pessoa_juridica(root: str = _CRAWL_ROOT) -> str:
    in_filepath = f"{root}/cad_adm_cart_pj.csv"
    ou_filepath = f"{root}/cleaned/bd_pessoa_juridica.csv"

    dataframe = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    dataframe.columns = [
        "cnpj",
        "denominacao_social",
        "denominacao_comercial",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
        "subcategoria_registro",
        "controle_acionario",
        "tipo_endereco",
        "logradouro",
        "complemento",
        "bairro",
        "municipio",
        "sigla_uf",
        "cep",
        "ddd",
        "telefone",
        "valor_patrimonial_liquido",
        "data_patrimonio_liquido",
        "email",
        "website",
    ]

    dataframe["cnpj"] = (
        dataframe["cnpj"]
        .str.replace(".", "", regex=False)
        .str.replace("/", "", regex=False)
        .str.replace("-", "", regex=False)
    )

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False)
    return ou_filepath


_CLEANERS = {
    RESPONSAVEL_TABLE_ID: clean_table_responsavel,
    PESSOA_FISICA_TABLE_ID: clean_table_pessoa_fisica,
    PESSOA_JURIDICA_TABLE_ID: clean_table_pessoa_juridica,
}

_COVERAGE = {
    RESPONSAVEL_TABLE_ID: RESPONSAVEL_COVERAGE,
    PESSOA_FISICA_TABLE_ID: PESSOA_FISICA_COVERAGE,
    PESSOA_JURIDICA_TABLE_ID: PESSOA_JURIDICA_COVERAGE,
}


def _count_csv_rows(filepath: str) -> int:
    """Conta linhas de dado (exclui o cabeçalho) num CSV já limpo."""
    with open(filepath, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def _count_published_rows(table_id: str) -> int:
    """Conta quantas linhas a tabela já tem publicada em prod."""
    client = bigquery.Client(project=_BQ_PROJECT)
    query = (
        f"SELECT COUNT(*) AS n FROM `{_BQ_PROJECT}.{DATASET_ID}.{table_id}`"
    )
    rows = list(client.query(query).result())
    return rows[0]["n"]


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        crawl()
        filepath = _CLEANERS[table_id]()

        source_rows = _count_csv_rows(filepath)
        published_rows = _count_published_rows(table_id)

        has_new_data = source_rows > published_rows
        reference_date = (
            datetime.date.today() if has_new_data else _NO_NEW_DATA_DATE
        )
        return SourceInspection(
            reference_date=reference_date, compare_against="table_update"
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa de novo, de forma independente do check_update.
        crawl()
        filepath = _CLEANERS[table_id]()

        return ExtractAndLoad(
            coverage=_COVERAGE[table_id].model_dump(),
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
