"""`download` tem que recusar um corpo menor que o Content-Length.

O gov.br corta conexões longas e o requests devolve o corpo truncado sem erro;
o arquivo só falhava depois, como BadZipFile, e o mês era perdido.
"""

import pytest

from pipelines.datasets.br_senatran_estatisticas.breakdowns import download

URL = "https://www.gov.br/x/frota_cor_jan_2019.xlsx"


class _Resposta:
    def __init__(self, corpo: bytes, headers: dict[str, str]):
        self.content = corpo
        self.headers = headers

    def raise_for_status(self) -> None:
        pass


class _Sessao:
    def __init__(self, respostas: list[_Resposta]):
        self.respostas = respostas
        self.chamadas = 0

    def get(self, url, headers=None, timeout=None):
        resposta = self.respostas[min(self.chamadas, len(self.respostas) - 1)]
        self.chamadas += 1
        return resposta


def test_tenta_de_novo_quando_o_corpo_vem_truncado(tmp_path):
    sessao = _Sessao(
        [
            _Resposta(b"abc", {"Content-Length": "6"}),
            _Resposta(b"abcdef", {"Content-Length": "6"}),
        ]
    )
    destino = download(URL, tmp_path, session=sessao)
    assert destino.read_bytes() == b"abcdef"
    assert sessao.chamadas == 2


def test_falha_se_todas_as_tentativas_vem_truncadas(tmp_path):
    sessao = _Sessao([_Resposta(b"abc", {"Content-Length": "6"})])
    with pytest.raises(RuntimeError, match="truncado"):
        download(URL, tmp_path, session=sessao)


def test_ignora_content_length_de_corpo_comprimido(tmp_path):
    sessao = _Sessao(
        [
            _Resposta(
                b"abcdef",
                {"Content-Length": "3", "Content-Encoding": "gzip"},
            )
        ]
    )
    assert download(URL, tmp_path, session=sessao).read_bytes() == b"abcdef"
