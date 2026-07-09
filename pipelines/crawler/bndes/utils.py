"""
Funcoes puras do crawler br_bndes_operacoes_contratadas.

Tres responsabilidades:
  - get_source_last_modified: le o sinal de atualizacao (CKAN last_modified) -> poll
  - download_csv: baixa o CSV consolidado (stream p/ disco + resume via Range)
  - clean: le o CSV bruto e grava Parquet particionado por ano (schema explicito)

Passo a passo de implementacao de cada funcao: ver task_davi/ROADMAP.md, secao 2.
"""

import shutil
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.crawler.bndes.constants import constants
from pipelines.utils.utils import log

CHUNKSIZE = 200_000


def get_source_last_modified() -> datetime:
    """
    Le o `last_modified` do recurso no CKAN (sinal de atualizacao do poll).

    Faz GET em constants.RESOURCE_SHOW_URL e parseia
    result["last_modified"] com constants.LAST_MODIFIED_FORMAT.

    Returns:
        datetime: Data/hora da ultima publicacao do CSV no portal.
    """
    response = httpx.get(constants.RESOURCE_SHOW_URL.value)

    response.raise_for_status()

    last_modified_date_iso = response.json()["result"]["last_modified"]

    last_modified_date = datetime.strptime(
        last_modified_date_iso, constants.LAST_MODIFIED_FORMAT.value
    )

    log(f"Fonte last_modified: {last_modified_date}")

    return last_modified_date


def parse_decimal_ptbr(s: pd.Series) -> pd.Series:
    """
    Converte decimal pt-BR do CSV (so virgula, SEM milhar) para float.

    Vale p/ valor_operacao, valor_desembolsado e taxa_juros: no CSV do dados
    abertos nao ha separador de milhar, entao NAO se remove '.'. Identico ao
    parse ja validado em task_davi/verify_csv_vs_xlsx.py.

    Args:
        s (pd.Series): Serie de strings (ex.: "8750,50").

    Returns:
        pd.Series: Serie float64 (NaN onde nao parseia).
    """

    s = s.str.replace(",", ".", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    return s


def download_csv(
    dest: Path, url: str | None = None, chunk_size: int = 1024 * 1024
) -> Path:
    """
    Baixa o CSV consolidado streamando p/ disco, com resume via Range.

    Memoria constante (nao carrega o 1,2 GB na RAM). Se `dest` ja existe
    parcialmente, retoma de onde parou pedindo `Range: bytes=<n>-`. Resposta
    206 (Partial Content) -> append a partir da posicao atual; resposta 200 ->
    o servidor ignorou o Range, rebaixar do zero (modo "wb"). Ao validar o
    tamanho final, lembrar que em 206 o Content-Length do response e o do trecho
    pedido, nao o do arquivo inteiro.

    Args:
        dest (Path): Caminho de destino do arquivo .csv.
        url (str | None): URL de download; default constants.DOWNLOAD_URL.
        chunk_size (int): Tamanho do bloco de escrita (bytes).

    Returns:
        Path: O proprio `dest`, ja com o arquivo completo.
    """

    Path.mkdir(dest.parent, parents=True, exist_ok=True)

    bytes_downloaded = dest.stat().st_size if dest.is_file() else 0

    headers = (
        {"Range": f"bytes={bytes_downloaded}-"} if bytes_downloaded else {}
    )

    with httpx.stream(
        method="GET", url=url, headers=headers, timeout=20
    ) as response:
        if response.status_code == 200:
            mode = "wb"
        elif response.status_code == 206:
            mode = "ab"
        else:
            response.raise_for_status()

        with open(dest, mode=mode) as fd:
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                fd.write(chunk)

        if response.status_code == 200:
            file_length = int(response.headers["Content-Length"])

        if response.status_code == 206:
            file_length = int(
                str(response.headers["Content-Range"]).split("/")[-1]
            )

        if file_length != dest.stat().st_size:
            raise httpx.HTTPError(
                "Download não pode ser finalizado mesmo com várias tentativas."
            )

        log("Download finalizado com sucesso!")

        return dest


def _transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as transformacoes de limpeza a um chunk do CSV (tudo string na entrada).

    Portado de task_davi/clean.py, adaptado ao CSV do dados abertos.

    Args:
        df (pd.DataFrame): Chunk cru lido com dtype=str.

    Returns:
        pd.DataFrame: Chunk limpo, com as colunas de constants.ORDER_COLUMNS
            (inclui `ano` derivado de data_contratacao).
    """

    df_dropped_cols = df.drop(columns=constants.DROP_COLUMNS.value)

    df_renamed_cols = df_dropped_cols.rename(columns=constants.RENAME.value)

    df_striped = df_renamed_cols.apply(lambda col: col.str.strip())

    df_striped["fonte_recurso"] = df_striped["fonte_recurso"].replace(
        "-", pd.NA
    )

    df_striped[["valor_operacao", "valor_desembolsado", "taxa_juros"]] = (
        df_striped[
            ["valor_operacao", "valor_desembolsado", "taxa_juros"]
        ].apply(parse_decimal_ptbr)
    )

    df_striped[["prazo_carencia", "prazo_amortizacao"]] = df_striped[
        ["prazo_carencia", "prazo_amortizacao"]
    ].apply(lambda col: pd.to_numeric(col, errors="coerce").astype("Int64"))

    date = pd.to_datetime(
        df_striped["data_contratacao"], format="%Y-%m-%d", errors="coerce"
    )

    df_striped["data_contratacao"] = date.dt.date

    df_striped["ano"] = date.dt.year.astype("Int64")

    df_striped["id_municipio"] = df_striped["id_municipio"].str.replace(
        r"\.0$", "", regex=True
    )

    condition = df_striped["id_municipio"].str.fullmatch(r"\d{7}")

    df_striped["id_municipio"] = df_striped["id_municipio"].where(
        condition, pd.NA
    )

    return df_striped[constants.ORDER_COLUMNS.value]


def clean(csv_path: Path, output_dir: Path) -> Path:
    """
    Le o CSV bruto em chunks e grava Parquet particionado por ano.

    Le com read_csv(sep=";", encoding="cp1252", dtype=str, chunksize=CHUNKSIZE),
    limpa cada chunk com _transform_chunk e vai anexando cada ano num
    pq.ParquetWriter proprio (um por particao, mantido aberto entre chunks) ->
    memoria constante. O schema explicito (constants.SCHEMA) garante que anos
    espalhados por varios chunks gravem tipos consistentes.

    Args:
        csv_path (Path): CSV bruto baixado.
        output_dir (Path): Raiz de saida; grava output_dir/ano=<ano>/data.parquet.

    Returns:
        Path: `output_dir` (raiz das particoes gravadas).
    """
    file_cols = [c for c in constants.ORDER_COLUMNS.value if c != "ano"]

    shutil.rmtree(output_dir, ignore_errors=True)

    writers = {}

    for chunk in pd.read_csv(
        csv_path, sep=";", encoding="cp1252", dtype=str, chunksize=CHUNKSIZE
    ):
        df = _transform_chunk(chunk)

        for year, group in df.groupby("ano"):
            if int(year) not in writers:
                Path.mkdir(
                    output_dir / f"ano={int(year)}/",
                    parents=True,
                    exist_ok=True,
                )

                writers[int(year)] = pq.ParquetWriter(
                    output_dir / f"ano={int(year)}/data.parquet",
                    constants.SCHEMA.value,
                    compression="snappy",
                )

            table = pa.Table.from_pandas(
                group[file_cols],
                schema=constants.SCHEMA.value,
                preserve_index=False,
            )

            writers[int(year)].write_table(table)

    for write in writers.values():
        write.close()

    return output_dir
