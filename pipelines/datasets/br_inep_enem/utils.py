"""
Download, limpeza e particionamento de br_inep_enem.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib3.exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.datasets.br_inep_enem.constants import constants

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"User-Agent": "Mozilla/5.0"}


def build_session() -> requests.Session:
    """Sessão que repete quando o servidor do INEP derruba a conexão.

    O `download.inep.gov.br` recusa conexão com frequência, às vezes já no
    handshake. A repetição fica aqui, e não só no `@task`, por dois motivos: ela
    também vale para quem chama estas funções na mão, e uma repetição no nível do
    Prefect recomeçaria o download inteiro.

    Returns:
        A sessão, com repetição e espera crescente entre tentativas.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def build_paths(table_id: str, clear_input: bool = False) -> tuple[Path, Path]:
    """Cria os diretórios de trabalho da tabela.

    O `output/` é apagado a cada chamada, de modo que sobra de uma execução
    anterior não entre no upload seguinte. O `input/` só é apagado no download,
    porque a limpeza precisa do que ele acabou de baixar — mas num backfill, que
    percorre vários anos, ele tem de ser esvaziado entre um ano e outro.

    Args:
        table_id: Slug da tabela.
        clear_input: Se True, apaga também o `input/`.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(constants.PATH.value) / table_id
    input_dir, output_dir = base / "input", base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    if clear_input:
        shutil.rmtree(input_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def get_source_max_date() -> str:
    """Lê na página do INEP o ano mais recente publicado.

    O valor é a edição do exame, não a data da consulta, e é o que o flow
    compara com a cobertura da tabela.

    Returns:
        O ano mais recente, no formato `%Y`.

    Raises:
        ValueError: Se a página não listar nenhum zip de microdados.
    """
    response = build_session().get(
        constants.SOURCE_LINK.value,
        headers=HEADERS,
        verify=False,
        timeout=120,
    )
    response.raise_for_status()

    anos = re.findall(r"microdados_enem_(\d{4})\.zip", response.text)
    if not anos:
        raise ValueError(
            f"nenhum microdados_enem_<ano>.zip em {constants.SOURCE_LINK.value}"
        )
    return max(anos)


def resolve_years(
    backfill_years: list[str] | None, source_max_date: str
) -> list[str]:
    """Decide quais edições a execução vai carregar.

    Sem backfill é só a edição corrente da fonte. Com backfill, são as edições
    pedidas, em ordem, e todas precisam existir no formato novo.

    Args:
        backfill_years: Edições a recarregar, no formato `%Y`, ou None.
        source_max_date: Edição mais recente publicada, no formato `%Y`.

    Returns:
        As edições a carregar, em ordem crescente.

    Raises:
        ValueError: Se alguma edição pedida for anterior à primeira publicada no
            formato novo, ou posterior à última publicada.
    """
    if not backfill_years:
        return [source_max_date]

    anos = sorted(set(backfill_years))
    fora = [
        ano
        for ano in anos
        if not (constants.FIRST_YEAR.value <= int(ano) <= int(source_max_date))
    ]
    if fora:
        raise ValueError(
            f"edições fora da faixa publicada no formato novo "
            f"({constants.FIRST_YEAR.value} a {source_max_date}): {fora}. "
            "Antes disso a fonte publica um arquivo único, que é a tabela "
            "microdados."
        )
    return anos


def download_table(table_id: str, ano: str) -> Path:
    """Baixa o zip da edição e extrai só o CSV que a tabela usa.

    O zip traz também dicionário, editais e scripts de leitura, que não entram.

    Args:
        table_id: Slug da tabela.
        ano: Edição a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com o CSV extraído.

    Raises:
        FileNotFoundError: Se o zip não tiver o CSV esperado da tabela.
    """
    input_dir, _ = build_paths(table_id, clear_input=True)
    url = constants.DOWNLOAD_LINK.value.format(ano=ano)
    zip_path = input_dir / f"microdados_enem_{ano}.zip"
    prefixo = constants.TABLES.value[table_id]["file_prefix"]

    with build_session().get(
        url, headers=HEADERS, verify=False, stream=True, timeout=30 * 60
    ) as response:
        response.raise_for_status()
        with open(zip_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=15 * 1024 * 1024):
                file.write(chunk)

    with ZipFile(zip_path) as archive:
        membros = [
            nome
            for nome in archive.namelist()
            if nome.rsplit("/", 1)[-1].upper().startswith(f"{prefixo}_")
            and nome.upper().endswith(".CSV")
        ]
        if len(membros) != 1:
            raise FileNotFoundError(
                f"esperava um CSV começando com {prefixo}_ em {url}, "
                f"achei {len(membros)}: {membros}"
            )
        alvo = membros[0]
        with archive.open(alvo) as origem:
            destino = input_dir / alvo.rsplit("/", 1)[-1]
            with open(destino, "wb") as file:
                shutil.copyfileobj(origem, file, length=15 * 1024 * 1024)

    zip_path.unlink()
    return input_dir


def read_chunks(path: Path, table_id: str) -> Iterator[pd.DataFrame]:
    """Lê o CSV em blocos, já renomeado e na ordem da arquitetura.

    Tudo entra como texto: a staging é toda STRING por convenção da casa, e o
    `.sql` faz `safe_cast` de cada coluna. Ler como texto também evita que o
    pandas transforme código de município em float e devolva `3550308.0`.

    Args:
        path: CSV da fonte.
        table_id: Slug da tabela.

    Yields:
        Cada bloco com as colunas da arquitetura, na ordem dela.
    """
    rename = constants.RENAME.value[table_id]
    columns = constants.COLUMNS.value[table_id]

    for chunk in pd.read_csv(
        path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        dtype=str,
        usecols=lambda name: name in rename,
        chunksize=constants.CHUNK_SIZE.value,
    ):
        chunk = chunk.rename(columns=rename)
        for column in constants.BOOLEAN_COLUMNS.value & set(chunk.columns):
            chunk[column] = chunk[column].map({"0": "false", "1": "true"})
        yield chunk[columns]


def clean_table(table_id: str, ano: str) -> Path:
    """Lê o CSV da edição e grava o particionado.

    O arquivo é escolhido pelo ano, e não pelo primeiro que aparece: num
    backfill o `input/` já teve outras edições.

    Args:
        table_id: Slug da tabela.
        ano: Edição a limpar, no formato `%Y`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.

    Raises:
        FileNotFoundError: Se o CSV da edição não estiver em `input/`.
    """
    input_dir, output_dir = build_paths(table_id)
    prefixo = constants.TABLES.value[table_id]["file_prefix"]

    arquivos = [
        arquivo
        for arquivo in sorted(input_dir.glob("*.csv"))
        if arquivo.name.upper().startswith(f"{prefixo}_{ano}")
    ]
    if len(arquivos) != 1:
        raise FileNotFoundError(
            f"esperava um {prefixo}_{ano}*.csv em {input_dir}, "
            f"achei {len(arquivos)}: {[a.name for a in arquivos]}"
        )

    write_partitions(
        read_chunks(arquivos[0], table_id),
        constants.TABLES.value[table_id]["partition_columns"],
        output_dir,
    )
    return output_dir


def write_partitions(
    chunks: Iterator[pd.DataFrame],
    partition_columns: list[str],
    output_dir: Path,
) -> int:
    """Grava os blocos em partições Hive, um `data.parquet` por partição.

    A gravação é em fluxo, com um `ParquetWriter` aberto por partição: o CSV de
    resultados tem 1,7 GB e não cabe em memória de uma vez. Um arquivo só por
    partição também é o que impede a tabela externa de duplicar linhas quando
    uma execução seguinte produz um número diferente de blocos.

    Args:
        chunks: Blocos já renomeados e na ordem da arquitetura.
        partition_columns: Colunas que compõem o caminho da partição. Vazio
            grava direto na raiz, para tabela sem partição.
        output_dir: Raiz do particionado.

    Returns:
        Quantas linhas foram gravadas.
    """
    writers: dict[Path, pq.ParquetWriter] = {}
    written = 0

    try:
        for chunk in chunks:
            grupos = (
                [((), chunk)]
                if not partition_columns
                else chunk.groupby(partition_columns, dropna=False)
            )
            for keys, group in grupos:
                keys = keys if isinstance(keys, tuple) else (keys,)
                partition = output_dir.joinpath(
                    *(
                        f"{col}={key}"
                        for col, key in zip(
                            partition_columns, keys, strict=True
                        )
                    )
                )
                partition.mkdir(parents=True, exist_ok=True)

                group = group.drop(columns=partition_columns)
                schema = pa.schema(
                    [(name, pa.string()) for name in group.columns]
                )
                table = pa.Table.from_pandas(
                    group, schema=schema, preserve_index=False
                )
                if partition not in writers:
                    writers[partition] = pq.ParquetWriter(
                        partition / "data.parquet",
                        schema,
                        compression="snappy",
                    )
                writers[partition].write_table(table)
                written += len(group)
    finally:
        for writer in writers.values():
            writer.close()

    return written
