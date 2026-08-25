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


class TestStreamingWrites:
    """clean_all must not hold the whole extraction in memory.

    As Python dicts, servidor_remuneracao and servidor_hora_extra_dia alone come
    to roughly 9 GB over full history — against a 4 GiB worker — so the time
    series are written a year at a time. This pins the mechanism that allows it:
    a second write with reset=False must add a partition rather than replace the
    table.
    """

    def test_reset_false_appends_a_partition(self, tmp_path):
        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            utils,
        )

        def rows(year: int, n: int) -> list[dict]:
            return [
                {
                    "ano": year,
                    "mes": 1,
                    "id_despesa": str(i),
                    "valor_reembolsado": 1.0,
                }
                for i in range(n)
            ]

        out = str(tmp_path)
        utils.write_partitioned(rows(2024, 3), out, "despesa_ceaps", "ano")
        utils.write_partitioned(
            rows(2025, 2), out, "despesa_ceaps", "ano", reset=False
        )

        table_dir = tmp_path / "despesa_ceaps"
        parts = sorted(p.name for p in table_dir.iterdir())
        assert parts == ["ano=2024", "ano=2025"], "the first year must survive"

        # and the default still replaces, so a re-run cannot leave stale years
        utils.write_partitioned(rows(2026, 1), out, "despesa_ceaps", "ano")
        assert sorted(p.name for p in table_dir.iterdir()) == ["ano=2026"]


class TestFanOutFailuresAreNotSilent:
    """A crawl that drops parents must say so, not return short data.

    The contratação fan-out is ~61k requests over ~12.7k parents. Before this,
    a parent whose request failed after all retries was recorded as "no
    children", so a network blip produced a table short by an unknown amount
    that looked perfectly healthy.
    """

    def test_a_few_failures_warn_but_return(self, caplog):
        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            senado_adm_api as api,
        )

        def flaky(item: int):
            if item == 7:
                raise RuntimeError("connection reset")
            return [item]

        with caplog.at_level("WARNING"):
            out = api.fan_out(range(200), flaky, workers=4, label="itens")

        assert len(out) == 200
        assert sum(1 for _, r in out if r is None) == 1
        assert "1/200 requests failed" in caplog.text

    def test_a_broadly_failing_crawl_raises(self):
        import pytest

        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            senado_adm_api as api,
        )

        def broken(item: int):
            raise RuntimeError("host unreachable")

        with pytest.raises(RuntimeError, match="silently incomplete"):
            api.fan_out(range(50), broken, workers=4, label="itens")


class TestSenadorNormalization:
    """The senador dimension is keyed by id_senador the admin API doesn't expose.

    Names are crosswalked to the legislative API. The normalization must be
    accent- and case-insensitive, and the tables keyed by id_senador must resolve
    only through the crosswalk, never carry a stray name column.
    """

    def test_name_normalization_is_accent_case_insensitive(self):
        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            senado_adm_clean as clean,
        )

        assert clean.norm_nome("Esperidião Amin") == "ESPERIDIAO AMIN"
        assert clean.norm_nome("  confúcio   moura ") == "CONFUCIO MOURA"
        assert clean.norm_nome("---") is None
        assert clean.norm_nome(None) is None

    def test_gabinete_resolves_id_via_crosswalk_only(self, monkeypatch):
        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            senado_adm_clean as clean,
        )

        monkeypatch.setattr(
            clean.api,
            "fetch",
            lambda path, *a, **k: [
                {
                    "nomeParlamentar": "ESPERIDIÃO AMIN",
                    "endereco": "Anexo 2",
                    "telefones": "(61) 0000",
                    "fax": "-",
                    "chefeGabinete": "Fulano",
                }
            ],
        )
        rows = clean.build_senador_gabinete(
            "2026-08-25", {"ESPERIDIAO AMIN": "22"}
        )

        assert rows[0]["id_senador"] == "22"
        # identity columns must be gone — only the office fields remain
        assert set(rows[0]) == {
            "data_extracao",
            "id_senador",
            "endereco",
            "telefones",
            "fax",
            "chefe_gabinete",
        }

    def test_dimension_flags_em_exercicio_and_keeps_historical(
        self, monkeypatch
    ):
        from pipelines.datasets.br_senado_dados_abertos_administrativos import (
            senado_adm_clean as clean,
        )

        core = {
            "22": {
                "id_senador": "22",
                "nome_parlamentar": "Esperidião Amin",
                "nome_completo": "Esperidião Amin H. Filho",
                "sexo": "Masculino",
            },
            "3": {
                "id_senador": "3",
                "nome_parlamentar": "Antonio Valadares",
                "nome_completo": "Antonio C. Valadares",
                "sexo": "Masculino",
            },
        }
        crosswalk = {"ESPERIDIAO AMIN": "22", "ANTONIO VALADARES": "3"}
        monkeypatch.setattr(
            clean.api,
            "fetch",
            lambda path, *a, **k: [
                {
                    "nomeParlamentar": "Esperidião Amin",
                    "uf": "SC",
                    "partido": "PP",
                    "titularSuplente": "Titular",
                    "mandato": "2019 / 2027",
                    "dataNascimento": "21/12/1947",
                    "email": "x@senado",
                }
            ],
        )
        rows = {
            r["id_senador"]: r
            for r in clean.build_senador("2026-08-25", (core, crosswalk))
        }

        assert rows["22"]["indicador_em_exercicio"] == "sim"
        assert rows["22"]["sigla_uf"] == "SC"
        assert rows["22"]["data_nascimento"] == "1947-12-21"
        # historical senator kept, current-only fields null
        assert rows["3"]["indicador_em_exercicio"] == "nao"
        assert rows["3"]["sigla_uf"] is None
        assert rows["3"]["nome_completo"] == "Antonio C. Valadares"
