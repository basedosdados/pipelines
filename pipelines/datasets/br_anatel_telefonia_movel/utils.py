"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""

from __future__ import annotations

import re
from io import SEEK_SET, BufferedReader, RawIOBase
from os import makedirs
from os.path import join
from typing import TYPE_CHECKING
from zipfile import ZipFile

import polars as pl
from httpx import Client

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log

if TYPE_CHECKING:
    from collections.abc import Buffer


class HttpRangeFile(RawIOBase):
    """Arquivo remoto lido por faixas de bytes, com a interface de `io.RawIOBase`.

    Cada leitura pede ao servidor só o trecho que começa na posição atual,
    pelo cabeçalho HTTP `Range`.

    Args:
        client: Cliente httpx usado nas requisições.
        url: Endereço do arquivo remoto.

    Raises:
        httpx.HTTPStatusError: Se o servidor responder ao `HEAD` com erro.
        OSError: Se o servidor não informar o tamanho do arquivo no `HEAD`.
    """

    def __init__(self, client: Client, url: str) -> None:
        super().__init__()
        self.client = client
        self.url = url
        response = self.client.head(self.url).raise_for_status()
        content_length = response.headers.get("content-length")

        if content_length is None:
            raise OSError("O servidor não informou o tamanho do arquivo.")

        self.position = 0
        self.size = int(content_length)

    def readable(self) -> bool:
        """Informa que o arquivo aceita leitura."""
        return True

    def seekable(self) -> bool:
        """Informa que o arquivo aceita mudar a posição de leitura."""
        return True

    def tell(self) -> int:
        """Devolve a posição de leitura atual, em bytes do começo do arquivo."""
        return self.position

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        """Move a posição de leitura e devolve a posição nova.

        Args:
            offset: Quantos bytes andar. Negativo anda para trás.
            whence: De onde contar: 0 (`SEEK_SET`) do começo, 1 (`SEEK_CUR`)
                da posição atual e 2 (`SEEK_END`) do fim do arquivo.

        Returns:
            A posição nova, em bytes contados do começo do arquivo.

        Raises:
            ValueError: Se `whence` não for 0, 1 ou 2, ou se a posição nova
                ficar negativa.
        """

        new_position: int

        match whence:
            case 0:
                new_position = offset
            case 1:
                new_position = self.position + offset
            case 2:
                new_position = self.size + offset
            case _:
                raise ValueError(f"Valor de whence inválido: {whence}")

        if new_position < 0:
            raise ValueError(f"Valor de posição negativo: {new_position}")

        self.position = new_position

        return new_position

    def readinto(self, buffer: Buffer) -> int:
        """Preenche o buffer com os bytes que começam na posição atual.

        Pede ao servidor um trecho com no máximo o tamanho do buffer.

        Args:
            buffer: Área de memória a preencher, a partir do começo.

        Returns:
            Quantos bytes foram escritos no buffer, ou 0 no fim do arquivo.

        Raises:
            httpx.HTTPStatusError: Se o servidor responder com erro.
            OSError: Se o servidor ignorar o cabeçalho `Range` e não responder 206.
        """
        if self.position >= self.size:
            return 0

        memory_view_buffer = memoryview(buffer)

        start: int = self.position
        end: int = min(
            self.position + len(memory_view_buffer) - 1, self.size - 1
        )

        headers = {"Range": f"bytes={start}-{end}"}

        with self.client.stream(
            "GET", url=self.url, headers=headers
        ) as response:
            response.raise_for_status()

            if response.status_code != 206:
                raise OSError("O servidor ignorou o Range.")

            data = response.read()
            n = len(data)

        memory_view_buffer[:n] = data

        self.position += n

        return n


def normalize_label(column: str) -> pl.Expr:
    """Normaliza os rótulos de uma coluna para comparação robusta.

    Retira os espaços nas pontas, converte para minúsculas e tira os acentos,
    de modo que variações como ``"Município"``, ``"MUNICIPIO"`` ou
    ``" municipio "`` comparem como iguais. Usado nos filtros de geografia
    (Brasil/UF/Município) para não quebrarem caso a Anatel revise a grafia dos
    rótulos.

    Args:
        column: Nome da coluna com os rótulos vindos da fonte.

    Returns:
        A expressão do polars com os rótulos normalizados.
    """

    return (
        pl.col(column)
        .str.strip_chars()
        .str.to_lowercase()
        .str.replace_many(
            ["á", "à", "â", "ã", "é", "ê", "í", "ó", "ô", "õ", "ú", "ü", "ç"],
            ["a", "a", "a", "a", "e", "e", "i", "o", "o", "o", "u", "u", "c"],
        )
    )


def get_zip_url() -> str:
    """Busca na API do dados.gov.br a URL do ZIP com os dados de acessos.

    Returns:
        A URL do primeiro recurso em formato CSV do conjunto.

    Raises:
        httpx.HTTPStatusError: Se a API responder com erro.
        ValueError: Se a API não listar nenhum recurso em formato CSV.
    """

    with Client(
        cookies=anatel_constants.COOKIES.value,
        headers=anatel_constants.HEADERS.value,
        timeout=10,
        follow_redirects=True,
    ) as client:
        response = (
            client.get(anatel_constants.URL.value).raise_for_status().json()
        )

        for resource in response["resources"]:
            if resource["format"] == "CSV":
                return resource["url"]

        raise ValueError("A API não trouxe um CSV.")


def get_csv_file_from_zip(names: list[str], table_id: str) -> str:
    """Escolhe, entre os arquivos do ZIP, o CSV que a tabela lê.

    As tabelas de densidade leem o `Densidade_Telefonia_Movel.csv`. O microdados
    lê o semestre mais recente, no formato `Acessos_Telefonia_Movel_2026_1S.csv`.
    Como o formato do nome é fixo, a ordem alfabética coincide com a do tempo.

    Args:
        names: Nomes dos arquivos do ZIP, como devolve `ZipFile.namelist()`.
        table_id: Tabela que vai ler o arquivo.

    Returns:
        O nome do arquivo dentro do ZIP.

    Raises:
        ValueError: Se o ZIP não tiver o arquivo que a tabela lê.
    """

    selected_names = []

    if table_id != "microdados" and "Densidade_Telefonia_Movel.csv" in names:
        return "Densidade_Telefonia_Movel.csv"
    elif table_id == "microdados":
        for name in names:
            if re.fullmatch(r"Acessos_Telefonia_Movel_\d{4}_[12]S\.csv", name):
                selected_names.append(name)

    if selected_names == []:
        raise ValueError(f"Não há CSVs para a tabela: {table_id}")

    most_recent_microdados_csv = max(selected_names)

    return most_recent_microdados_csv


def download_csv_from_zip(table_id: str) -> str:
    """Baixa do ZIP da Anatel só o CSV que a tabela lê.

    O ZIP é lido direto no servidor, por faixas de bytes, sem ser baixado
    inteiro. Cada requisição traz até 8 MiB, e o arquivo escolhido é gravado em
    `INPUT_PATH` com o mesmo nome que tem dentro do ZIP.

    Args:
        table_id: Tabela que vai ler o arquivo.

    Returns:
        O caminho do CSV gravado.

    Raises:
        httpx.HTTPStatusError: Se a API ou o servidor da Anatel responderem com
            erro.
        OSError: Se o servidor da Anatel não informar o tamanho do ZIP ou
            ignorar o cabeçalho `Range`.
        ValueError: Se a API não listar o ZIP ou se o ZIP não tiver o arquivo
            que a tabela lê.
    """

    url = get_zip_url()

    with Client(
        cookies=anatel_constants.COOKIES.value,
        headers=anatel_constants.HEADERS.value,
        timeout=10,
        follow_redirects=True,
    ) as client:
        buffer_size = 8 * 2**20

        anatel_file = BufferedReader(HttpRangeFile(client, url), buffer_size)

        with ZipFile(anatel_file) as zip_file:
            csv_file = get_csv_file_from_zip(zip_file.namelist(), table_id)

            log(
                f"Baixando {csv_file} de {url} em {anatel_constants.INPUT_PATH.value}"
            )

            csv_path = zip_file.extract(
                csv_file, path=anatel_constants.INPUT_PATH.value
            )

            return csv_path


def clean_csv_microdados(csv_path: str, table_id: str) -> None:

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    renamed_df = df.rename(anatel_constants.RENAME_COLUMNS_MICRODADOS.value)

    selected_cols_df = renamed_df.with_columns(
        pl.col("produto").str.to_lowercase()
    ).select(anatel_constants.ORDER_COLUMNS_MICRODADOS.value)

    one_month_per_row_table = (
        selected_cols_df.select("ano", "mes").unique().collect()
    )

    for year, month in one_month_per_row_table.iter_rows():
        path = join(
            anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
            f"ano={year}",
            f"mes={month}",
        )

        makedirs(path, exist_ok=True)

        df_without_year_and_month = selected_cols_df.filter(
            (pl.col("ano") == year) & (pl.col("mes") == month)
        ).drop("ano", "mes")

        df_without_year_and_month.sink_csv(join(path, "data.csv"))


def clean_csv_densidade(csv_path: str, table_id: str) -> None:

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    level = anatel_constants.DENSITY_GEOGRAPHIC_LEVEL.value[table_id]

    filtered_df = df.filter(
        normalize_label("Nível Geográfico Densidade") == level
    )

    renamed_df = filtered_df.rename(
        anatel_constants.RENAME_COLUMNS_DENSIDADE.value
    )

    cleaned_df = renamed_df.with_columns(
        pl.col("densidade").str.replace(",", ".")
    ).select(anatel_constants.DENSITY_COLUMNS.value[table_id])

    makedirs(
        anatel_constants.TABLES_OUTPUT_PATH.value[table_id], exist_ok=True
    )

    cleaned_df.sink_csv(
        join(
            anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
            f"{table_id}.csv",
        )
    )
