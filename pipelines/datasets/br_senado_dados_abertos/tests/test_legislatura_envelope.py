"""Testes da leitura do roster por legislatura e da UF de `senador`, sem rede."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from pipelines.datasets.br_senado_dados_abertos import utils
from pipelines.datasets.br_senado_dados_abertos.utils import (
    _parlamentar_uf,
    _parse_xml_records,
    _xml_to_dict,
)

ROOTLESS = (
    "<Parlamentar><IdentificacaoParlamentar>"
    "<CodigoParlamentar>5918</CodigoParlamentar>"
    "<NomeParlamentar>Adilson Gomes</NomeParlamentar>"
    "</IdentificacaoParlamentar></Parlamentar>"
    "<Parlamentar><IdentificacaoParlamentar>"
    "<CodigoParlamentar>4545</CodigoParlamentar>"
    "<NomeParlamentar>Jarbas Vasconcelos</NomeParlamentar>"
    "</IdentificacaoParlamentar></Parlamentar>"
)

ENVELOPED = (
    "<ListaParlamentarLegislatura><Parlamentares>"
    "<Parlamentar><IdentificacaoParlamentar>"
    "<CodigoParlamentar>5918</CodigoParlamentar>"
    "</IdentificacaoParlamentar></Parlamentar>"
    "</Parlamentares></ListaParlamentarLegislatura>"
)


def test_rootless_concatenation_yields_every_record():
    """Um corpo sem raiz devolve todos os registros."""
    records = _parse_xml_records(ROOTLESS, "Parlamentar")

    assert len(records) == 2
    codes = [
        r["IdentificacaoParlamentar"]["CodigoParlamentar"] for r in records
    ]
    assert codes == ["5918", "4545"]


def test_enveloped_body_still_parses():
    """Um corpo com o envelope `ListaParlamentarLegislatura` também é lido."""
    records = _parse_xml_records(ENVELOPED, "Parlamentar")

    assert len(records) == 1
    assert (
        records[0]["IdentificacaoParlamentar"]["CodigoParlamentar"] == "5918"
    )


def test_xml_declaration_is_tolerated():
    """A declaração `<?xml ...?>` no início do corpo é ignorada."""
    body = '<?xml version="1.0" encoding="UTF-8"?>' + ROOTLESS

    assert len(_parse_xml_records(body, "Parlamentar")) == 2


def test_repeated_siblings_become_a_list():
    """Tags irmãs repetidas viram lista."""
    elem = _parse_xml_records(
        "<Mandatos><Mandato><Uf>PE</Uf></Mandato>"
        "<Mandato><Uf>SC</Uf></Mandato></Mandatos>",
        "Mandatos",
    )[0]

    assert elem["Mandato"] == [{"Uf": "PE"}, {"Uf": "SC"}]


def test_single_child_stays_scalar():
    """Uma tag sem irmã repetida continua dicionário, como no envelope JSON."""
    elem = _parse_xml_records(
        "<Mandatos><Mandato><Uf>PE</Uf></Mandato></Mandatos>", "Mandatos"
    )[0]

    assert elem["Mandato"] == {"Uf": "PE"}


def test_empty_leaf_becomes_none():
    """Folha vazia vira None; o texto da folha sai sem espaços nas pontas."""
    assert _xml_to_dict(ET.fromstring("<Email></Email>")) is None
    assert _xml_to_dict(ET.fromstring("<Email>  a@b  </Email>")) == "a@b"


def test_unparseable_body_names_the_record_tag():
    """Corpo ilegível levanta erro com a tag procurada na mensagem."""
    with pytest.raises(RuntimeError, match="Parlamentar"):
        _parse_xml_records("<Parlamentar><broken", "Parlamentar")


def test_empty_roster_raises_instead_of_shipping_a_thin_table(monkeypatch):
    """Sem parlamentar em nenhuma legislatura, `clean_senador` levanta erro."""
    monkeypatch.setattr(utils, "_legislatura_parlamentares", lambda leg: [])

    with pytest.raises(RuntimeError, match="no parlamentares returned"):
        utils.clean_senador()


def test_persistent_empty_200_is_an_empty_roster_not_a_failure(monkeypatch):
    """Resposta 200 vazia em todas as tentativas vira roster vazio."""

    class _Empty:
        status_code = 200
        text = ""

    monkeypatch.setattr(utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(utils.httpx, "get", lambda *a, **k: _Empty())

    assert utils.get_xml_records("/x", "Parlamentar", retries=2) == []


def test_persistent_non_200_still_raises(monkeypatch):
    """Status diferente de 200 em todas as tentativas levanta erro."""

    class _ServerError:
        status_code = 503
        text = ""

    monkeypatch.setattr(utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(utils.httpx, "get", lambda *a, **k: _ServerError())

    with pytest.raises(RuntimeError, match="503"):
        utils.get_xml_records("/x", "Parlamentar", retries=2)


def test_uf_comes_from_the_mandate_when_identification_omits_it():
    """A UF vem do mandato quando a identificação não traz UF."""
    record = _parse_xml_records(
        "<Parlamentar>"
        "<IdentificacaoParlamentar>"
        "<CodigoParlamentar>5918</CodigoParlamentar>"
        "</IdentificacaoParlamentar>"
        "<Mandatos><Mandato><UfParlamentar>PE</UfParlamentar></Mandato></Mandatos>"
        "</Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) == "PE"


def test_uf_takes_the_mandate_when_the_two_sources_contradict():
    """Com UFs diferentes no mandato e na identificação, vale a do mandato."""
    record = _parse_xml_records(
        "<Parlamentar>"
        "<IdentificacaoParlamentar><UfParlamentar>SC</UfParlamentar>"
        "</IdentificacaoParlamentar>"
        "<Mandatos><Mandato><UfParlamentar>PE</UfParlamentar></Mandato></Mandatos>"
        "</Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) == "PE"


def test_uf_falls_back_to_identification_without_a_mandate():
    """Sem `Mandatos`, a UF vem da identificação."""
    record = _parse_xml_records(
        "<Parlamentar><IdentificacaoParlamentar>"
        "<UfParlamentar>BA</UfParlamentar>"
        "</IdentificacaoParlamentar></Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) == "BA"


def test_uf_is_none_when_neither_source_carries_one():
    """Sem UF no mandato nem na identificação, a UF é None."""
    record = _parse_xml_records(
        "<Parlamentar><IdentificacaoParlamentar>"
        "<CodigoParlamentar>1</CodigoParlamentar>"
        "</IdentificacaoParlamentar></Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) is None


def test_row_is_kept_whole_when_a_senator_changed_uf(monkeypatch):
    """A linha mais completa é mantida inteira, com a UF da sua legislatura."""
    current_leg = utils.current_legislatura()

    def _roster(leg: int) -> list[dict]:
        if leg == current_leg:
            return _parse_xml_records(
                "<Parlamentar><IdentificacaoParlamentar>"
                "<CodigoParlamentar>1</CodigoParlamentar>"
                "</IdentificacaoParlamentar>"
                "<Mandatos><Mandato><UfParlamentar>RJ</UfParlamentar>"
                "</Mandato></Mandatos></Parlamentar>",
                "Parlamentar",
            )
        if leg == current_leg - 1:
            return _parse_xml_records(
                "<Parlamentar><IdentificacaoParlamentar>"
                "<CodigoParlamentar>1</CodigoParlamentar>"
                "<NomeParlamentar>Fulano</NomeParlamentar>"
                "<NomeCompletoParlamentar>Fulano de Tal</NomeCompletoParlamentar>"
                "<SexoParlamentar>Masculino</SexoParlamentar>"
                "<FormaTratamento>Senador </FormaTratamento>"
                "<SiglaPartidoParlamentar>XYZ</SiglaPartidoParlamentar>"
                "<EmailParlamentar>f@senado.leg.br</EmailParlamentar>"
                "</IdentificacaoParlamentar>"
                "<Mandatos><Mandato><UfParlamentar>SP</UfParlamentar>"
                "</Mandato></Mandatos></Parlamentar>",
                "Parlamentar",
            )
        return []

    monkeypatch.setattr(utils, "_legislatura_parlamentares", _roster)

    df = utils.clean_senador()

    assert len(df) == 1
    assert df.loc[0, "sigla_uf"] == "SP"
    assert df.loc[0, "nome"] == "Fulano"
