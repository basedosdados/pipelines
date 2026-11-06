"""Testes do `HttpRangeFile`, com um servidor falso do httpx, sem rede."""

from io import SEEK_CUR, SEEK_END

import httpx
import pytest

from pipelines.datasets.br_anatel_telefonia_movel.utils import HttpRangeFile

URL = "https://www.anatel.gov.br/acessos_telefonia_movel.zip"
CONTENT = bytes(range(100))


class FakeServer:
    """Servidor falso que serve `CONTENT` e guarda os `Range` pedidos.

    Args:
        honor_range: Se responde 206 com o trecho pedido. Se não, responde 200
            com o arquivo inteiro.
        send_length: Se o `HEAD` traz o `content-length`.
    """

    def __init__(
        self, honor_range: bool = True, send_length: bool = True
    ) -> None:
        self.honor_range = honor_range
        self.send_length = send_length
        self.ranges: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        """Responde a uma requisição do cliente."""
        if request.method == "HEAD":
            if not self.send_length:
                return httpx.Response(200)
            return httpx.Response(
                200, headers={"content-length": str(len(CONTENT))}
            )

        range_header = request.headers["Range"]
        self.ranges.append(range_header)

        if not self.honor_range:
            return httpx.Response(200, content=CONTENT)

        bounds = range_header.removeprefix("bytes=").split("-")
        start = int(bounds[0])
        end = int(bounds[1])

        return httpx.Response(206, content=CONTENT[start : end + 1])


def open_remote_file(server: FakeServer) -> HttpRangeFile:
    """Abre o `HttpRangeFile` de `URL` num cliente que fala com o servidor falso."""
    client = httpx.Client(transport=httpx.MockTransport(server))
    return HttpRangeFile(client, URL)


class TestInit:
    """Testes do `HEAD` que o `HttpRangeFile` faz ao abrir."""

    def test_reads_size_from_content_length(self) -> None:
        """O tamanho do arquivo vem do `content-length` do `HEAD`."""
        remote_file = open_remote_file(FakeServer())

        assert remote_file.size == len(CONTENT)
        assert remote_file.tell() == 0

    def test_raises_when_head_has_no_content_length(self) -> None:
        """Sem `content-length` no `HEAD`, levanta `OSError` em vez de `KeyError`."""
        server = FakeServer(send_length=False)

        with pytest.raises(OSError, match="tamanho"):
            open_remote_file(server)


class TestSeek:
    """Testes de `seek` com cada `whence`."""

    def test_seek_set_counts_from_start(self) -> None:
        """`SEEK_SET` vai para o `offset` contado do começo."""
        remote_file = open_remote_file(FakeServer())

        assert remote_file.seek(10) == 10
        assert remote_file.tell() == 10

    def test_seek_cur_counts_from_current_position(self) -> None:
        """`SEEK_CUR` anda a partir da posição atual, para frente e para trás."""
        remote_file = open_remote_file(FakeServer())
        remote_file.seek(10)

        assert remote_file.seek(5, SEEK_CUR) == 15
        assert remote_file.seek(-15, SEEK_CUR) == 0

    def test_seek_end_counts_from_end(self) -> None:
        """`SEEK_END` conta a partir do fim do arquivo."""
        remote_file = open_remote_file(FakeServer())

        assert remote_file.seek(-10, SEEK_END) == len(CONTENT) - 10
        assert remote_file.seek(0, SEEK_END) == len(CONTENT)

    def test_rejects_negative_position(self) -> None:
        """Uma posição antes do começo do arquivo levanta `ValueError`."""
        remote_file = open_remote_file(FakeServer())

        with pytest.raises(ValueError, match="negativo"):
            remote_file.seek(-1)

    def test_rejects_invalid_whence(self) -> None:
        """Um `whence` fora de 0, 1 e 2 levanta `ValueError`."""
        remote_file = open_remote_file(FakeServer())

        with pytest.raises(ValueError, match="whence"):
            remote_file.seek(0, 3)


class TestReadinto:
    """Testes de `readinto`, que pede um trecho do arquivo por `Range`."""

    def test_reads_from_middle_of_file(self) -> None:
        """No meio do arquivo, pede e preenche exatamente o tamanho do buffer."""
        server = FakeServer()
        remote_file = open_remote_file(server)
        remote_file.seek(10)
        buffer = bytearray(20)

        assert remote_file.readinto(buffer) == 20
        assert bytes(buffer) == CONTENT[10:30]
        assert server.ranges == ["bytes=10-29"]
        assert remote_file.tell() == 30

    def test_reads_only_what_remains_at_end(self) -> None:
        """Perto do fim, pede só até o último byte e devolve quantos leu."""
        server = FakeServer()
        remote_file = open_remote_file(server)
        remote_file.seek(90)
        buffer = bytearray(20)

        assert remote_file.readinto(buffer) == 10
        assert bytes(buffer[:10]) == CONTENT[90:]
        assert server.ranges == ["bytes=90-99"]
        assert remote_file.tell() == len(CONTENT)

    def test_returns_zero_at_end_of_file(self) -> None:
        """No fim do arquivo, devolve 0 sem fazer requisição."""
        server = FakeServer()
        remote_file = open_remote_file(server)
        remote_file.seek(0, SEEK_END)

        assert remote_file.readinto(bytearray(20)) == 0
        assert server.ranges == []

    def test_raises_when_server_ignores_range(self) -> None:
        """Se o servidor responde 200 em vez de 206, levanta `OSError`."""
        remote_file = open_remote_file(FakeServer(honor_range=False))
        buffer = bytearray(20)

        with pytest.raises(OSError, match="Range"):
            remote_file.readinto(buffer)

    def test_reads_whole_file_in_chunks_unchanged(self) -> None:
        """Lido em trechos de 16 bytes, o arquivo sai igual, sem byte repetido."""
        server = FakeServer()
        remote_file = open_remote_file(server)
        chunks = []

        chunk = remote_file.read(16)
        while chunk:
            chunks.append(chunk)
            chunk = remote_file.read(16)

        assert b"".join(chunks) == CONTENT
        assert server.ranges == [
            "bytes=0-15",
            "bytes=16-31",
            "bytes=32-47",
            "bytes=48-63",
            "bytes=64-79",
            "bytes=80-95",
            "bytes=96-99",
        ]
