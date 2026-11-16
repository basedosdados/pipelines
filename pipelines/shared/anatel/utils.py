"""
Funções compartilhadas pelos conjuntos da Anatel
(`br_anatel_banda_larga_fixa`, `br_anatel_telefonia_movel`).
"""

from __future__ import annotations

from io import SEEK_SET, RawIOBase
from typing import TYPE_CHECKING

from httpx import Client

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
