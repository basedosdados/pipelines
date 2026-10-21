"""
Tasks for br_cgu_emendas_parlamentares.

A fonte (Portal da Transparência) não expõe nenhuma data de última
atualização — o único sinal real de dado novo é comparar o número de linhas
do CSV baixado com o da tabela `microdados` já materializada em prod. Por
isso `get_latest_update` precisa baixar o .zip inteiro só pra descobrir se
há novidade (a variante "check_and_download" que trataria esse caso à parte
foi cancelada — decisão: todo dataset usa check_update -> extract_and_load,
mesmo quando o check precisa baixar dado pra descobrir a data).
`extract_load_data` baixa a fonte de novo, de forma independente — custo
duplicado aceito.
"""

import os
import time
import zipfile
from datetime import date, datetime
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from prefect import task

from pipelines.datasets.br_cgu_emendas_parlamentares.constants import (
    COVERAGE,
    constants,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection
from pipelines.utils.utils import log

INPUT = Path("/tmp/input/")
OUTPUT = Path("/tmp/output/")


def download_unzip_file(
    input_dir: Path | str = INPUT, timeout: int = 60, retries: int = 2
) -> None:
    """Baixa o .zip de Emendas Parlamentares do Portal da Transparência e o extrai.
    Em caso de falha na requisição, tenta de novo com espera exponencial. O .zip é removido após a extração.

    Args:
        input_dir: Diretório onde o .zip é salvo e extraído. Criado se não
            existir.
        timeout: Tempo máximo, em segundos, de cada requisição.
        retries: Número de novas tentativas após a primeira falha.

    Raises:
        RuntimeError: Se todas as tentativas de download falharem.
        zipfile.BadZipFile: Se a resposta não for um .zip válido.
    """
    input_dir = Path(input_dir)
    input_dir.mkdir(exist_ok=True, parents=True)

    for attempt in range(retries + 1):
        try:
            r = requests.get(
                url=constants.URL.value,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=timeout,
                verify=False,
            )
            r.raise_for_status()
            with open(  # nosec
                input_dir / "emendas_parlamentares.zip", "wb"
            ) as code:
                code.write(r.content)

            with zipfile.ZipFile(
                input_dir / "emendas_parlamentares.zip", "r"
            ) as zp:
                zp.extractall(input_dir)

            os.remove(input_dir / "emendas_parlamentares.zip")
            log("Arquivo baixado e descompactado com sucesso", level="info")
            return
        except requests.RequestException as e:
            if attempt == retries:
                raise RuntimeError(
                    f"Falha ao baixar {constants.URL.value}: {e}"
                ) from e
            time.sleep(2**attempt)


@task
def convert_str_to_float(
    input_dir: Path | str = INPUT, output_dir: Path | str = OUTPUT
) -> Path:
    """Converte as colunas de valor para float e salva o CSV de microdados.

    Troca a vírgula decimal por ponto nas colunas de `constants.VALUES_FLOAT` e grava
    `microdados.csv`com UTF-8.

    Args:
        input_dir: Diretório com o `EmendasParlamentares.csv` extraído.
        output_dir: Diretório de saída do `microdados.csv`. Criado se não
            existir.

    Returns:
        O diretório de saída, pronto para ser passado a `upload_to_gcs`.
    """
    input_dir = Path(input_dir)
    df = pd.read_csv(
        input_dir / "EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )
    log("Convertendo valores para float")

    for _ in constants.VALUES_FLOAT.value:
        df[_] = df[_].str.replace(",", ".").astype(float)

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    output = output_dir / "microdados.csv"

    df.to_csv(output, sep=",", encoding="utf-8", index=False)
    log("---------------- Tabela salva -------------------")
    return output_dir


@task
def get_last_modified_time(input_dir: Path | str = INPUT) -> date:
    """Baixa a fonte e estima a data da última atualização dos dados, por meio
    da comparação do número de linhas do arquivo recém-baixado com o da tabela
    `basedosdados.br_cgu_emendas_parlamentares.microdados`.

    Args:
        input_dir: Diretório onde a fonte é baixada e extraída.

    Returns:
        A data de hoje se houver linhas novas; senão, o `creation_time` da
        tabela em prod, como `datetime.date`.
    """
    input_dir = Path(input_dir)
    download_unzip_file(input_dir)
    emendas = pd.read_csv(
        input_dir / "EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )

    data = bd.read_sql(
        constants.QUERY.value,
        billing_project_id="basedosdados",
        from_file=True,
    )

    row = data.iloc[0].to_numpy()
    prod_rows, prod_last_modified = row[0], row[1]
    log("Data da última atualização: " + str(prod_last_modified))
    log("Quantidade de linhas na tabela: " + str(prod_rows))
    log("Quantidade de linhas no arquivo: " + str(emendas.shape[0]))
    if emendas.shape[0] > prod_rows:
        return datetime.today().date()

    return prod_last_modified.date()


def get_latest_update() -> SourceInspection:
    """Descobre se há dado novo baixando a fonte inteira (ver banner do módulo).

    Returns:
        `SourceInspection` com a data resolvida por `get_last_modified_time`.
        `compare_against="table_update"`: o sinal não é uma competência (a
        fonte não tem coluna de data), e sim um timestamp de quando a
        mudança foi detectada — compatível com `Table.Update.latest`, não
        com `Coverage.DateTimeRange` (que é `ano_emenda`, granularidade
        anual).
    """
    reference_date = get_last_modified_time()
    return SourceInspection(
        reference_date=reference_date,
        compare_against="table_update",
    )


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Baixa a fonte de novo (independente do check) e gera o `microdados.csv`.

    Args:
        download_params: `{"reference_date": "<...>"}` — não usado: a fonte
            não é parametrizável por data, o download é sempre do mesmo
            endpoint único, e o CSV extraído já traz a base inteira.

    Returns:
        `ExtractAndLoad` com o diretório de `microdados.csv`.
    """
    download_unzip_file(INPUT)
    output_dir = convert_str_to_float()

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=str(output_dir),
        dump_mode="append",
        source_format="csv",
    )
