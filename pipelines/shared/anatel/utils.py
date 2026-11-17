"""
Funções compartilhadas pelos conjuntos da Anatel
(`br_anatel_banda_larga_fixa`, `br_anatel_telefonia_movel`).
"""

from __future__ import annotations

import re
from io import SEEK_SET, BufferedReader, RawIOBase
from typing import TYPE_CHECKING
from zipfile import ZipFile

import polars as pl
from httpx import Client

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
    ``" municipio "`` comparem como iguais.

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


def get_zip_url(
    api_url: str,
    resource_format: str,
    cookies: dict[str, str],
    headers: dict[str, str],
) -> str:
    """Busca na API do dados.gov.br a URL do ZIP com os dados de acessos.

    Args:
        api_url: Endereço do conjunto na API do dados.gov.br.
        resource_format: Formato do recurso que aponta para o ZIP, como a API
            o descreve (`"CSV"` ou `"ZIP"`).
        cookies: Cookies enviados à API.
        headers: Cabeçalhos enviados à API.

    Returns:
        A URL do primeiro recurso no formato pedido.

    Raises:
        httpx.HTTPStatusError: Se a API responder com erro.
        ValueError: Se a API não listar nenhum recurso no formato pedido.
    """

    with Client(
        cookies=cookies,
        headers=headers,
        timeout=10,
        follow_redirects=True,
    ) as client:
        response = client.get(api_url).raise_for_status().json()

        for resource in response["resources"]:
            if resource["format"] == resource_format:
                return resource["url"]

        raise ValueError(f"A API não trouxe um recurso {resource_format}.")


def get_csv_file_from_zip(
    names: list[str],
    table_id: str,
    microdados_pattern: str,
    densidade_file: str,
) -> str:
    """Escolhe, entre os arquivos do ZIP, o CSV que a tabela lê.

    As tabelas de densidade leem `densidade_file`. O microdados lê o arquivo
    mais recente entre os que casam com `microdados_pattern`. Como o formato do
    nome é fixo, a ordem alfabética coincide com a do tempo.

    Args:
        names: Nomes dos arquivos do ZIP, como devolve `ZipFile.namelist()`.
        table_id: Tabela que vai ler o arquivo.
        microdados_pattern: Expressão regular do nome dos CSVs do microdados,
            como `r"Acessos_Banda_Larga_Fixa_\\d{4}\\.csv"`.
        densidade_file: Nome do CSV das tabelas de densidade.

    Returns:
        O nome do arquivo dentro do ZIP.

    Raises:
        ValueError: Se o ZIP não tiver o arquivo que a tabela lê.
    """

    if table_id != "microdados":
        if densidade_file in names:
            return densidade_file

        raise ValueError(f"Não há CSVs para a tabela: {table_id}")

    microdados_names = [
        name for name in names if re.fullmatch(microdados_pattern, name)
    ]

    if microdados_names == []:
        raise ValueError(f"Não há CSVs para a tabela: {table_id}")

    return max(microdados_names)


def download_csv_from_zip(
    table_id: str,
    api_url: str,
    resource_format: str,
    microdados_pattern: str,
    densidade_file: str,
    cookies: dict[str, str],
    headers: dict[str, str],
    input_path: str,
) -> str:
    """Baixa do ZIP da Anatel só o CSV que a tabela lê.

    O ZIP é lido direto no servidor, por faixas de bytes, sem ser baixado
    inteiro. Cada requisição traz até 8 MiB, e o arquivo escolhido é gravado em
    `input_path` com o mesmo nome que tem dentro do ZIP.

    Args:
        table_id: Tabela que vai ler o arquivo.
        api_url: Endereço do conjunto na API do dados.gov.br.
        resource_format: Formato do recurso que aponta para o ZIP, como a API
            o descreve (`"CSV"` ou `"ZIP"`).
        microdados_pattern: Expressão regular do nome dos CSVs do microdados.
        densidade_file: Nome do CSV das tabelas de densidade.
        cookies: Cookies enviados à API e ao servidor da Anatel.
        headers: Cabeçalhos enviados à API e ao servidor da Anatel.
        input_path: Pasta onde o CSV é gravado.

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

    url = get_zip_url(
        api_url=api_url,
        resource_format=resource_format,
        cookies=cookies,
        headers=headers,
    )

    with Client(
        cookies=cookies,
        headers=headers,
        timeout=10,
        follow_redirects=True,
    ) as client:
        buffer_size = 8 * 2**20

        anatel_file = BufferedReader(HttpRangeFile(client, url), buffer_size)

        with ZipFile(anatel_file) as zip_file:
            csv_file = get_csv_file_from_zip(
                names=zip_file.namelist(),
                table_id=table_id,
                microdados_pattern=microdados_pattern,
                densidade_file=densidade_file,
            )

            log(f"Baixando {csv_file} de {url} em {input_path}")

            csv_path = zip_file.extract(csv_file, path=input_path)

            return csv_path
