"""Regression tests for the dados-abertos roster endpoint losing its envelope.

Between 2026-08-13 and 2026-08-20 `senador/lista/legislatura/{leg}` stopped
wrapping its records: the XML became a rootless concatenation of `<Parlamentar>`
elements, and the JSON serialization collapsed 245 senators into one. The armed
pipeline crashed with `KeyError: 'id_senador'` on the resulting empty frame.

These run offline against fixtures; the live shape was verified separately.
"""

from __future__ import annotations

import pytest

from pipelines.datasets.br_senado_dados_abertos.senado_api import (
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
    """The post-regression shape: N records, no wrapping root."""
    records = _parse_xml_records(ROOTLESS, "Parlamentar")

    assert len(records) == 2
    codes = [
        r["IdentificacaoParlamentar"]["CodigoParlamentar"] for r in records
    ]
    assert codes == ["5918", "4545"]


def test_enveloped_body_still_parses():
    """If the Senate restores the envelope, the same reader keeps working."""
    records = _parse_xml_records(ENVELOPED, "Parlamentar")

    assert len(records) == 1
    assert (
        records[0]["IdentificacaoParlamentar"]["CodigoParlamentar"] == "5918"
    )


def test_xml_declaration_is_tolerated():
    body = '<?xml version="1.0" encoding="UTF-8"?>' + ROOTLESS

    assert len(_parse_xml_records(body, "Parlamentar")) == 2


def test_repeated_siblings_become_a_list():
    """Matches what `_as_list` expects from the JSON envelope."""
    elem = _parse_xml_records(
        "<Mandatos><Mandato><Uf>PE</Uf></Mandato>"
        "<Mandato><Uf>SC</Uf></Mandato></Mandatos>",
        "Mandatos",
    )[0]

    assert elem["Mandato"] == [{"Uf": "PE"}, {"Uf": "SC"}]


def test_single_child_stays_scalar():
    """A lone repeated tag stays a dict, exactly as the JSON envelope collapses it."""
    elem = _parse_xml_records(
        "<Mandatos><Mandato><Uf>PE</Uf></Mandato></Mandatos>", "Mandatos"
    )[0]

    assert elem["Mandato"] == {"Uf": "PE"}


def test_empty_leaf_becomes_none():
    import xml.etree.ElementTree as ET

    assert _xml_to_dict(ET.fromstring("<Email></Email>")) is None
    assert _xml_to_dict(ET.fromstring("<Email>  a@b  </Email>")) == "a@b"


def test_unparseable_body_names_the_record_tag():
    with pytest.raises(RuntimeError, match="Parlamentar"):
        _parse_xml_records("<Parlamentar><broken", "Parlamentar")


def test_empty_roster_raises_instead_of_shipping_a_thin_table(monkeypatch):
    """An empty roster must abort, not write a truncated `senador` table.

    The original failure was `KeyError: 'id_senador'` on an empty frame. Merely
    tolerating the empty case would have let the lossy JSON shape through as a
    one-row roster, which is worse than crashing.
    """
    from pipelines.datasets.br_senado_dados_abertos import senado_clean

    monkeypatch.setattr(
        senado_clean, "_legislatura_parlamentares", lambda leg: []
    )

    with pytest.raises(RuntimeError, match="no parlamentares returned"):
        senado_clean.clean_senador()


def test_persistent_empty_200_is_an_empty_roster_not_a_failure(monkeypatch):
    """Legislatures predating the API answer 200 with a zero-byte body.

    Verified live: legislature 36 returns HTTP 200 / 0 bytes, legislature 40
    returns ~92 KB. Treating the empty one as an error aborted the whole run on
    the first such legislature — `get_json` already documented this case as a
    legitimately-empty list endpoint.
    """
    from pipelines.datasets.br_senado_dados_abertos import senado_api

    class _Empty:
        status_code = 200
        text = ""

    monkeypatch.setattr(senado_api.time, "sleep", lambda *_: None)
    monkeypatch.setattr(senado_api.requests, "get", lambda *a, **k: _Empty())

    assert senado_api.get_xml_records("/x", "Parlamentar", retries=2) == []


def test_persistent_non_200_still_raises(monkeypatch):
    """An empty body is benign; a real HTTP failure must not be swallowed."""
    from pipelines.datasets.br_senado_dados_abertos import senado_api

    class _ServerError:
        status_code = 503
        text = ""

    monkeypatch.setattr(senado_api.time, "sleep", lambda *_: None)
    monkeypatch.setattr(
        senado_api.requests, "get", lambda *a, **k: _ServerError()
    )

    with pytest.raises(RuntimeError, match="503"):
        senado_api.get_xml_records("/x", "Parlamentar", retries=2)


def test_uf_comes_from_the_mandate_when_identification_omits_it():
    """`Mandatos/Mandato/UfParlamentar` is populated where the identification is not.

    Reading only `IdentificacaoParlamentar/UfParlamentar` left `sigla_uf` filled
    on 97 of 1,567 senators (6%), just above the 5% floor the dbt proportion test
    enforces — so the gap never failed a run.
    """
    from pipelines.datasets.br_senado_dados_abertos.senado_clean import (
        _parlamentar_uf,
    )

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


def test_uf_prefers_the_mandate_but_they_agree_upstream():
    """Verified live: 151 records carry both, and all 151 match."""
    from pipelines.datasets.br_senado_dados_abertos.senado_clean import (
        _parlamentar_uf,
    )

    record = _parse_xml_records(
        "<Parlamentar>"
        "<IdentificacaoParlamentar><UfParlamentar>SC</UfParlamentar>"
        "</IdentificacaoParlamentar>"
        "<Mandatos><Mandato><UfParlamentar>SC</UfParlamentar></Mandato></Mandatos>"
        "</Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) == "SC"


def test_uf_falls_back_to_identification_without_a_mandate():
    """A record with no `Mandatos` still yields its identification UF."""
    from pipelines.datasets.br_senado_dados_abertos.senado_clean import (
        _parlamentar_uf,
    )

    record = _parse_xml_records(
        "<Parlamentar><IdentificacaoParlamentar>"
        "<UfParlamentar>BA</UfParlamentar>"
        "</IdentificacaoParlamentar></Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) == "BA"


def test_uf_is_none_when_neither_source_carries_one():
    from pipelines.datasets.br_senado_dados_abertos.senado_clean import (
        _parlamentar_uf,
    )

    record = _parse_xml_records(
        "<Parlamentar><IdentificacaoParlamentar>"
        "<CodigoParlamentar>1</CodigoParlamentar>"
        "</IdentificacaoParlamentar></Parlamentar>",
        "Parlamentar",
    )[0]

    assert _parlamentar_uf(record) is None
