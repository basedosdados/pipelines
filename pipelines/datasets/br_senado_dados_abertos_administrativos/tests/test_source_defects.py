"""Regression tests for the administrative API's known defects.

Each of these was found by probing the live API during onboarding and would
silently corrupt a table if the transform stopped compensating for it. They run
offline against fixtures shaped like the real payloads; the live behaviour is
recorded in models/br_senado_dados_abertos_administrativos/ONBOARDING_PLAN.md.
"""

from __future__ import annotations

from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_clean as clean,
)


class TestParsing:
    """The source mixes date, money and boolean encodings within one API."""

    def test_dates_arrive_in_two_formats(self):
        # contratações and supridos answer ISO; servidores and senadores dd/mm.
        assert clean.date("2024-11-05") == "2024-11-05"
        assert clean.date("23/10/1976") == "1976-10-23"

    def test_null_sentinels_are_not_values(self):
        # `---` is what the pensionista list puts in an open-ended dataFim.
        for sentinel in ("---", "-", "", "   "):
            assert clean.date(sentinel) is None
            assert clean.s(sentinel) is None

    def test_brazilian_and_json_number_formats(self):
        assert clean.num("16.368,74") == 16368.74
        assert clean.num("0,00") == 0.0
        assert clean.num(527) == 527.0
        assert clean.num(257.78) == 257.78

    def test_booleans_arrive_as_json_and_as_letters(self):
        assert clean.flag(True) == "sim"
        assert clean.flag(False) == "nao"
        assert clean.flag("S") == "sim"
        assert clean.flag("N") == "nao"

    def test_overtime_durations_parse_to_hours(self):
        assert clean.hours("02h00") == 2.0
        assert clean.hours("01h30") == 1.5


class TestDocumentosFiscaisRepeatPerPayment:
    """The contract's whole document list is repeated on every payment.

    Verified live on contract 2280, which returns the same five document ids on
    each of its four payments. Modelling them at payment grain would multiply
    the row count by the number of payments.
    """

    @staticmethod
    def _payment(pid: int) -> dict:
        docs = [
            {
                "id": i,
                "numero": str(i),
                "data_emissao": "2012-01-01",
                "data_vencimento": None,
            }
            for i in (417, 418, 419, 420, 1018)
        ]
        return {
            "id": pid,
            "observacao": None,
            "multa": "0,00",
            "glosa": "0,00",
            "descricao_despesa": "Serviço",
            "valor_cobrado": "1.000,00",
            "documentos_fiscais": docs,
            "tipo_contratacao": "contratos",
            "id_contratacao": 2280,
        }

    def test_documents_deduplicate_to_contract_grain(self, monkeypatch):
        payments = [self._payment(p) for p in (457, 458, 459, 460)]
        monkeypatch.setattr(
            clean.api, "fetch_sub_resource", lambda *a, **k: payments
        )
        monkeypatch.setattr(
            clean.api, "fetch_pagamento_empenhos", lambda *a, **k: []
        )

        pagamentos, documentos, _ = clean.build_contratacao_pagamento(
            [], "2026-08-24"
        )

        assert len(pagamentos) == 4, "one row per payment"
        # 4 payments x 5 documents = 20 before dedup; 5 after.
        assert len(documentos) == 5, "documents belong to the contract"
        assert {d["id_documento_fiscal"] for d in documentos} == {
            "417",
            "418",
            "419",
            "420",
            "1018",
        }
        assert all(d["id_contratacao"] == "2280" for d in documentos)
        assert "id_pagamento" not in documentos[0]


class TestSupridosRepeatMovimentacoes:
    """/supridos/{ano} repeats some movimentações verbatim inside one ato.

    Three of 836 in 2018, identical down to the ato de concessão. Left in, they
    double-count their value.
    """

    @staticmethod
    def _payload(dup_id: str) -> list[dict]:
        movimentacao = {
            "id": dup_id,
            "codigoAtoConcessao": "00012018",
            "tipoInscricao": "Pessoa jurídica",
            "inscricao": "02741001000112",
            "fornecedor": "LOJAO DAS FLORES EIRELI ME",
            "valor": 700.0,
            "numero": "00007662",
            "rubricas": "33903024",
            "tipo": "Nota fiscal",
            "data": "2018-01-11",
            "dataProcessamento": "2026-08-22 10:00:03",
            "subTipos": [
                {
                    "id": "627",
                    "descricaoTipoDespesa": "Material de Consumo",
                    "descricaoSubtipoDespesa": "Material elétrico",
                    "valor": 290,
                    "rubrica": "33903026",
                }
            ],
        }
        return [
            {
                "codigo": "80860",
                "nome": "Fulano",
                "orgao": {"codigo": 1, "sigla": "X", "nome": "Y"},
                "atosConcessao": [
                    {
                        "codigoAtoConcessao": "00012018",
                        "ano": 2018,
                        "data": "2018-01-05",
                        "empenhos": [],
                        "transacoes": [],
                        # the same movimentação twice, as the source returns it
                        "movimentacoes": [movimentacao, dict(movimentacao)],
                    }
                ],
            }
        ]

    def test_identical_movimentacoes_are_collapsed(self, monkeypatch):
        monkeypatch.setattr(
            clean.api, "fetch", lambda *a, **k: self._payload("1855")
        )
        tables = clean.build_supridos([2018])

        assert len(tables["suprido_movimentacao"]) == 1
        assert tables["suprido_movimentacao"][0]["valor"] == 700.0
        # the subtipos inherit the duplication and must collapse with it
        assert len(tables["suprido_movimentacao_subtipo"]) == 1


class TestCessaoDirection:
    """Three cessão endpoints, three shapes, and none states the direction.

    Origin and destination are reconstructed from which endpoint answered.
    """

    def test_direction_is_reconstructed_per_endpoint(self, monkeypatch):
        payloads = {
            "/servidores/cedidos/para-senado": [
                {
                    "nome": "A",
                    "matricula": 1,
                    "dataExercicio": "15/02/2019",
                    "lotacao": "Gab",
                    "orgaoOrigem": "TRF1",
                }
            ],
            "/servidores/cedidos/pelo-senado": [
                {
                    "nome": "B",
                    "matricula": 2,
                    "cargo": "ANALISTA",
                    "categoria": "AL",
                    "orgao": "CÂMARA DOS DEPUTADOS",
                }
            ],
            "/servidores/cedidos/infraero-para-senado": [
                {
                    "nome": "C",
                    "matricula": 3,
                    "dataExercicio": "04/02/2020",
                    "lotacao": "COAPAT",
                }
            ],
            "/servidores/exercicio-provisorio": [],
        }
        monkeypatch.setattr(
            clean.api, "fetch", lambda path, *a, **k: payloads[path]
        )
        rows = {
            r["nome"]: r for r in clean.build_servidor_cedido("2026-08-24")
        }

        assert rows["A"]["orgao_origem"] == "TRF1"
        assert rows["A"]["orgao_destino"] == clean.SENADO
        assert rows["B"]["orgao_origem"] == clean.SENADO
        assert rows["B"]["orgao_destino"] == "CÂMARA DOS DEPUTADOS"
        assert rows["C"]["orgao_origem"] == "INFRAERO"
        assert rows["C"]["orgao_destino"] == clean.SENADO


class TestQuadroPessoalConsolidation:
    """The six establishment reports fold into one table.

    The source's `variacao*` fields are dropped because they are the percentage
    change between the ANTERIOR and ATUAL rows the consolidation emits; this
    test pins that they really are derivable, so dropping them loses nothing.
    """

    def test_variacao_is_derivable_from_the_period_pair(self):
        # Real values from /servidores/quadro-servidores-estaveis-e-nao-estaveis.
        for before, after, published in ((717, 825, 15.1), (528, 475, -10.0)):
            derived = (after - before) / before * 100
            assert abs(derived - published) < 0.05

    def test_ant_and_hoje_become_two_periods(self, monkeypatch):
        row = {
            "categoria": "ADVOGADO",
            "nivel": "III",
            "especialidade": "ADVOGADO",
            "totalCargosHoje": 49,
            "totalOcupadosAnt": 44,
            "totalOcupadosHoje": 44,
            "totalVagosAnt": 5,
            "totalVagosHoje": 5,
        }
        monkeypatch.setattr(
            clean.api,
            "fetch",
            lambda path, *a, **k: (
                [row] if path == "/gestao/quadro-cargos-efetivos" else []
            ),
        )
        rows = clean.build_quadro_pessoal("2026-08-24")

        assert {r["periodo"] for r in rows} == {"ATUAL", "ANTERIOR"}
        atual = next(r for r in rows if r["periodo"] == "ATUAL")
        anterior = next(r for r in rows if r["periodo"] == "ANTERIOR")
        assert atual["quantidade_cargos"] == 49
        assert atual["quantidade_ocupados"] == 44
        # only the ATUAL period carries the authorised-post count
        assert anterior["quantidade_cargos"] is None
        assert anterior["quantidade_vagos"] == 5
        assert all(r["quadro"] == "cargo_efetivo" for r in rows)
