"""Testes dos defeitos conhecidos da API administrativa, sem rede."""

from __future__ import annotations

import datetime as dt

import pytest

from pipelines.datasets.br_senado_dados_abertos_administrativos import utils
from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    TABLES_CONSTANTS,
    constants_servidor_cedido,
)
from pipelines.utils.metadata import policy
from pipelines.utils.metadata.domain import PartBdpro
from pipelines.utils.metadata.policy import CoverageIds


def test_dates_arrive_in_two_formats():
    """Datas ISO e `dd/mm/aaaa` viram `aaaa-mm-dd`."""
    assert utils.normalize_date("2024-11-05") == "2024-11-05"
    assert utils.normalize_date("23/10/1976") == "1976-10-23"


def test_null_sentinels_are_not_values():
    """Os nulos da fonte viram None."""
    for sentinel in ["---", "-", "", "   "]:
        assert utils.normalize_date(sentinel) is None
        assert utils.cleaned_string(sentinel) is None


def test_brazilian_and_json_number_formats():
    """Números JSON e textos em formato brasileiro viram float."""
    assert utils.parse_number("16.368,74") == 16368.74
    assert utils.parse_number("0,00") == 0.0
    assert utils.parse_number(527) == 527.0
    assert utils.parse_number(257.78) == 257.78


def test_booleans_arrive_as_json_and_as_letters():
    """Booleanos JSON e as letras `S`/`N` viram `sim`/`nao`."""
    assert utils.normalize_flag(True) == "sim"
    assert utils.normalize_flag(False) == "nao"
    assert utils.normalize_flag("S") == "sim"
    assert utils.normalize_flag("N") == "nao"


def test_overtime_durations_parse_to_hours():
    """Durações `02h30` viram horas decimais."""
    assert utils.parse_hours("02h00") == 2.0
    assert utils.parse_hours("01h30") == 1.5


def _pagamento(id_pagamento: int) -> dict:
    """Monta um pagamento da contratação 2280 com os cinco documentos dela."""
    documentos = []
    for id_documento in [417, 418, 419, 420, 1018]:
        documentos.append(
            {
                "id": id_documento,
                "numero": str(id_documento),
                "data_emissao": "2012-01-01",
                "data_vencimento": None,
            }
        )
    return {
        "id": id_pagamento,
        "observacao": None,
        "multa": "0,00",
        "glosa": "0,00",
        "descricao_despesa": "Serviço",
        "valor_cobrado": "1.000,00",
        "documentos_fiscais": documentos,
        "tipo_contratacao": "contratos",
        "id_contratacao": 2280,
    }


def test_documentos_fiscais_deduplicate_to_contratacao(monkeypatch):
    """Os documentos repetidos em cada pagamento ficam um por contratação."""
    pagamentos = [
        _pagamento(id_pagamento) for id_pagamento in [457, 458, 459, 460]
    ]
    monkeypatch.setattr(
        utils,
        "fetch_sub_resource",
        lambda contratacoes, sub_resource, tipos=None: pagamentos,
    )
    monkeypatch.setattr(
        utils, "fetch_pagamento_empenhos", lambda pagamentos: []
    )

    rows, documentos, _ = utils.clean_contratacao_pagamento([], "2026-08-24")

    assert len(rows) == 4
    assert len(documentos) == 5
    assert {documento["id_documento_fiscal"] for documento in documentos} == {
        "417",
        "418",
        "419",
        "420",
        "1018",
    }
    assert all(
        documento["id_contratacao"] == "2280" for documento in documentos
    )
    assert "id_pagamento" not in documentos[0]


def _supridos_payload(id_movimentacao: str) -> list[dict]:
    """Monta um suprido de 2018 com a mesma movimentação repetida."""
    movimentacao = {
        "id": id_movimentacao,
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
                    "movimentacoes": [movimentacao, dict(movimentacao)],
                }
            ],
        }
    ]


def test_identical_movimentacoes_are_collapsed(monkeypatch):
    """Movimentações repetidas no mesmo ato, e seus subtipos, ficam uma só."""
    monkeypatch.setattr(
        utils,
        "fetch_records",
        lambda path, params=None: _supridos_payload("1855"),
    )
    tables = utils.clean_supridos([2018])

    assert len(tables["suprido_movimentacao"]) == 1
    assert tables["suprido_movimentacao"][0]["valor"] == 700.0
    assert len(tables["suprido_movimentacao_subtipo"]) == 1


def test_cessao_direction_is_reconstructed_per_endpoint(monkeypatch):
    """A origem e o destino da cessão saem do endpoint que respondeu."""
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
        utils, "fetch_records", lambda path, params=None: payloads[path]
    )
    rows = {}
    for row in utils.clean_servidor_cedido("2026-08-24"):
        rows[row["nome"]] = row
    senado_federal = constants_servidor_cedido.SENADO_FEDERAL.value

    assert rows["A"]["orgao_origem"] == "TRF1"
    assert rows["A"]["orgao_destino"] == senado_federal
    assert rows["B"]["orgao_origem"] == senado_federal
    assert rows["B"]["orgao_destino"] == "CÂMARA DOS DEPUTADOS"
    assert rows["C"]["orgao_origem"] == "INFRAERO"
    assert rows["C"]["orgao_destino"] == senado_federal


def test_variacao_is_derivable_from_the_period_pair():
    """A variação publicada pela fonte sai do par `ANTERIOR` e `ATUAL`."""
    for before, after, published in [(717, 825, 15.1), (528, 475, -10.0)]:
        derived = (after - before) / before * 100
        assert abs(derived - published) < 0.05


def test_quadro_pessoal_splits_ant_and_hoje_into_two_periods(monkeypatch):
    """Os campos `...Ant` e `...Hoje` viram os períodos `ANTERIOR` e `ATUAL`."""
    record = {
        "categoria": "ADVOGADO",
        "nivel": "III",
        "especialidade": "ADVOGADO",
        "totalCargosHoje": 49,
        "totalOcupadosAnt": 44,
        "totalOcupadosHoje": 44,
        "totalVagosAnt": 5,
        "totalVagosHoje": 5,
    }

    def fake_fetch_records(path: str, params: dict | None = None) -> list:
        """Responde só o quadro de cargos efetivos."""
        if path == "/gestao/quadro-cargos-efetivos":
            return [record]
        return []

    monkeypatch.setattr(utils, "fetch_records", fake_fetch_records)
    rows = utils.clean_quadro_pessoal("2026-08-24")

    assert {row["periodo"] for row in rows} == {"ATUAL", "ANTERIOR"}
    atual = next(row for row in rows if row["periodo"] == "ATUAL")
    anterior = next(row for row in rows if row["periodo"] == "ANTERIOR")
    assert atual["quantidade_cargos"] == 49
    assert atual["quantidade_ocupados"] == 44
    assert anterior["quantidade_cargos"] is None
    assert anterior["quantidade_vagos"] == 5
    assert all(row["quadro"] == "cargo_efetivo" for row in rows)


def _despesa_rows(year: int, total: int) -> list[dict]:
    """Monta `total` linhas de `despesa_ceaps` de um ano."""
    rows = []
    for index in range(total):
        rows.append(
            {
                "ano": year,
                "mes": 1,
                "id_despesa": str(index),
                "valor_reembolsado": 1.0,
            }
        )
    return rows


def test_write_table_without_reset_appends_a_partition(tmp_path):
    """Com `reset=False`, a gravação acrescenta a partição às já gravadas."""
    out_root = str(tmp_path)
    utils.write_table(out_root, "despesa_ceaps", _despesa_rows(2024, 3))
    utils.write_table(
        out_root, "despesa_ceaps", _despesa_rows(2025, 2), reset=False
    )

    table_dir = tmp_path / "despesa_ceaps"
    partitions = sorted(path.name for path in table_dir.iterdir())
    assert partitions == ["ano=2024", "ano=2025"]

    utils.write_table(out_root, "despesa_ceaps", _despesa_rows(2026, 1))
    assert sorted(path.name for path in table_dir.iterdir()) == ["ano=2026"]


def test_empty_window_writes_no_parquet(tmp_path):
    """Uma tabela particionada sem linhas não grava parquet."""
    utils.write_table(str(tmp_path), "suprido_movimentacao", [])

    table_dir = tmp_path / "suprido_movimentacao"
    files = list(table_dir.rglob("*.parquet")) if table_dir.exists() else []
    assert files == []


def test_fan_out_with_few_failures_warns_and_returns(caplog):
    """Poucas falhas viram aviso, e o item que falhou volta com None."""

    def flaky(item: int) -> list[int]:
        """Falha só no item 7."""
        if item == 7:
            raise RuntimeError("connection reset")
        return [item]

    with caplog.at_level("WARNING"):
        results = utils.fan_out(range(200), flaky, workers=4, label="itens")

    assert len(results) == 200
    assert sum(1 for _, result in results if result is None) == 1
    assert "1/200 requests failed" in caplog.text


def test_fan_out_with_many_failures_raises():
    """Falhas acima do limite levantam exceção."""

    def broken(item: int) -> list[int]:
        """Falha sempre."""
        raise RuntimeError("host unreachable")

    with pytest.raises(RuntimeError, match="silently incomplete"):
        utils.fan_out(range(50), broken, workers=4, label="itens")


def test_name_normalization_is_accent_and_case_insensitive():
    """A normalização de nomes ignora acento, caixa e espaços extras."""
    assert utils.normalize_name("Esperidião Amin") == "ESPERIDIAO AMIN"
    assert utils.normalize_name("  confúcio   moura ") == "CONFUCIO MOURA"
    assert utils.normalize_name("---") == ""
    assert utils.normalize_name(None) == ""


def test_gabinete_resolves_id_senador_through_the_crosswalk(monkeypatch):
    """`senador_gabinete` acha o `id_senador` pelo nome e não guarda o nome."""
    monkeypatch.setattr(
        utils,
        "fetch_records",
        lambda path, params=None: [
            {
                "nomeParlamentar": "ESPERIDIÃO AMIN",
                "endereco": "Anexo 2",
                "telefones": "(61) 0000",
                "fax": "-",
                "chefeGabinete": "Fulano",
            }
        ],
    )
    rows = utils.clean_senador_gabinete(
        "2026-08-25", {"ESPERIDIAO AMIN": "22"}
    )

    assert rows[0]["id_senador"] == "22"
    assert set(rows[0]) == {
        "data_extracao",
        "id_senador",
        "endereco",
        "telefones",
        "fax",
        "chefe_gabinete",
    }


def test_senador_flags_em_exercicio_and_keeps_historical(monkeypatch):
    """`senador` marca quem está em exercício e mantém os históricos."""
    identificacoes = {
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
        utils,
        "fetch_records",
        lambda path, params=None: [
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
    rows = {}
    for row in utils.clean_senador(identificacoes, crosswalk):
        rows[row["id_senador"]] = row

    assert rows["22"]["indicador_em_exercicio"] == "sim"
    assert rows["22"]["sigla_uf"] == "SC"
    assert rows["22"]["data_nascimento"] == "1947-12-21"
    assert rows["3"]["indicador_em_exercicio"] == "nao"
    assert rows["3"]["sigla_uf"] is None
    assert rows["3"]["nome_completo"] == "Antonio C. Valadares"


def test_coverage_covers_every_table_but_dicionario():
    """Só o `dicionario`, que não tem flow, fica sem cobertura."""
    uncovered = set()
    for table_id, table_constants in TABLES_CONSTANTS.items():
        if table_constants.COVERAGE.value is None:
            uncovered.add(table_id)

    assert uncovered == {"dicionario"}


def test_no_coverage_inverts_the_pro_range():
    """Nenhuma cobertura `PartBdpro` gera faixa paga invertida."""
    ids = CoverageIds(
        free="11111111-1111-1111-1111-111111111111",
        pro="22222222-2222-2222-2222-222222222222",
    )
    for table_constants in TABLES_CONSTANTS.values():
        spec = table_constants.COVERAGE.value
        if not isinstance(spec, PartBdpro):
            continue
        for source_date in [
            dt.date(2026, 1, 1),
            dt.date(2026, 7, 15),
            dt.date(2026, 12, 31),
        ]:
            ranges = policy.compute_coverage_ranges(spec, source_date, ids)
            assert ranges.pro is not None
            pro = ranges.pro.model_dump(exclude_none=True)
            start = (
                pro.get("startYear"),
                pro.get("startMonth", 1),
                pro.get("startDay", 1),
            )
            end = (
                pro.get("endYear"),
                pro.get("endMonth", 12),
                pro.get("endDay", 28),
            )
            assert start <= end, (spec.date_column, source_date, start, end)
