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
