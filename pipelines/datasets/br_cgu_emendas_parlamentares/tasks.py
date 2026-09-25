import os
import time
import zipfile
from datetime import datetime
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from prefect import task

from pipelines.datasets.br_cgu_emendas_parlamentares.constants import constants
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
def get_last_modified_time(input_dir: Path | str = INPUT) -> datetime:
    """Baixa a fonte e estima a data da última atualização dos dados, por meio
    da comparação do número de linhas do arquivo recém-baixado com o da tabela
    `basedosdados.br_cgu_emendas_parlamentares.microdados`.

    Args:
        input_dir: Diretório onde a fonte é baixada e extraída.

    Returns:
        `datetime.today()` se houver linhas novas; senão, o
        `creation_time` da tabela em prod como `pd.Timestamp`.
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

    date = data.iloc[0].to_numpy()
    log("Data da última atualização: " + str(date[1]))
    log("Quantidade de linhas na tabela: " + str(date[0]))
    log("Quantidade de linhas no arquivo: " + str(emendas.shape[0]))
    if emendas.shape[0] > date[0]:
        return datetime.today()

    return date[1]
