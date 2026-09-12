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


def test_uf_takes_the_mandate_when_the_two_sources_contradict():
    """Pins the precedence, using a contradiction the live API does not produce.

    Upstream the two always agree — 151 records carry both across legislatures
    30, 40, 50, 56 and 57, and all 151 match — so equal values would pass under
    an identification-first implementation too and prove nothing. Distinct
    values are synthetic on purpose: they fix which source wins if the Senate
    ever lets them diverge.
    """
    from pipelines.datasets.br_senado_dados_abertos.senado_clean import (
        _parlamentar_uf,
    )

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


def test_row_is_kept_whole_when_a_senator_changed_uf(monkeypatch):
    """The chosen row comes from one legislature; `sigla_uf` is not sourced apart.

    A senator whose older legislature has more fields populated keeps that
    row — and therefore its older UF — because `_score` outranks `_leg`. That
    is deliberate: sourcing `sigla_uf` from the newest legislature instead
    would yield a row matching no single legislature.

    Measured against the live API, the two rules never disagree: 9 of 1,567
    senators have more than one UF, and for all 9 the row chosen here already
    carries the most recent one. This test pins the semantics for the case the
    live data does not currently exercise.
    """
    from pipelines.datasets.br_senado_dados_abertos import senado_clean

    def _roster(leg: int) -> list[dict]:
        if leg == senado_clean.CURRENT_LEG:  # newest, but sparse
            return _parse_xml_records(
                "<Parlamentar><IdentificacaoParlamentar>"
                "<CodigoParlamentar>1</CodigoParlamentar>"
                "</IdentificacaoParlamentar>"
                "<Mandatos><Mandato><UfParlamentar>RJ</UfParlamentar>"
                "</Mandato></Mandatos></Parlamentar>",
                "Parlamentar",
            )
        if leg == senado_clean.CURRENT_LEG - 1:  # older, but complete
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

    monkeypatch.setattr(senado_clean, "_legislatura_parlamentares", _roster)

    df = senado_clean.clean_senador()

    assert len(df) == 1
    assert df.loc[0, "sigla_uf"] == "SP"
    assert df.loc[0, "nome"] == "Fulano"
