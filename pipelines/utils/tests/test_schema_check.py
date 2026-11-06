"""Testes para avaliar funcionalidade implementadas por pipelines/pipelines/utils/schema_check.py"""

from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipelines.utils.schema_check import (
    Column,
    SchemaMismatchError,
    StagingSchema,
    UploadSchema,
    check,
)
from pipelines.utils.tasks import _retry_unless_schema_mismatch


def schema(cls, *cols, keys=()):
    """Monta um schema a partir de nomes ou de tuplas (nome, tipo)."""
    columns = tuple(
        Column.of(*c) if isinstance(c, tuple) else Column.of(c) for c in cols
    )
    return cls(columns, keys)


def staging(*cols, keys=()):
    return schema(StagingSchema, *cols, keys=keys)


def upload(*cols, keys=()):
    return schema(UploadSchema, *cols, keys=keys)


## Testes sobre CSVs


# Testes com check()
def test_csv_sem_mudancas():
    r = check(staging("a", "b"), upload("a", "b"), "csv")
    assert not r.has_changes
    assert r.added_columns == ()


def test_csv_coluna_nova_no_fim_e_warning():
    r = check(staging("a", "b"), upload("a", "b", "c"), "csv")
    assert not r.errors
    assert len(r.warnings) == 1
    assert [c.name for c in r.added_columns] == ["c"]


def test_csv_coluna_nova_no_meio_e_erro():
    r = check(staging("a", "b"), upload("a", "c", "b"), "csv")
    assert r.errors


def test_csv_coluna_faltando_no_fim_e_erro():
    r = check(staging("a", "b", "c"), upload("a", "b"), "csv")
    assert r.errors


def test_csv_coluna_faltando_no_meio_e_erro():
    r = check(staging("a", "b", "c"), upload("a", "c"), "csv")
    assert r.errors


def test_csv_ordem_diferente_e_erro():
    r = check(staging("a", "b"), upload("b", "a"), "csv")
    assert r.errors
    assert "ordem" in r.errors[0]


def test_csv_renomeacao_e_erro():
    r = check(staging("a", "b"), upload("a", "z"), "csv")
    assert r.errors


def test_csv_nome_e_comparado_normalizado():
    r = check(staging("Nome Coluna"), upload("Nome_Coluna"), "csv")
    assert not r.has_changes


## Testes sobre Parquets


# Testes com check()
def test_parquet_coluna_nova_em_qualquer_posicao_e_warning():
    r = check(
        staging(("a", "STRING"), ("b", "STRING")),
        upload(("a", "STRING"), ("c", "STRING"), ("b", "STRING")),
        "parquet",
    )
    assert not r.errors
    assert r.warnings
    assert [c.name for c in r.added_columns] == ["c"]
    assert r.added_columns[0].data_type == "STRING"


def test_parquet_coluna_faltando_e_erro():
    r = check(staging("a", "b"), upload("a"), "parquet")
    assert r.errors


def test_parquet_ordem_diferente_e_so_warning():
    r = check(staging("a", "b"), upload("b", "a"), "parquet")
    assert not r.errors
    assert r.warnings


def test_parquet_tipo_incompativel_e_erro():
    r = check(staging(("a", "STRING")), upload(("a", "INT64")), "parquet")
    assert r.errors
    assert "a (STRING -> INT64)" in r.errors[0]


def test_parquet_aliases_de_tipo_sao_equivalentes():
    r = check(
        staging(("a", "INTEGER"), ("b", "FLOAT"), ("c", "BOOLEAN")),
        upload(("a", "INT64"), ("b", "FLOAT64"), ("c", "BOOL")),
        "parquet",
    )
    assert not r.has_changes


def test_parquet_tipo_desconhecido_nao_gera_erro():
    r = check(staging(("a", "STRING")), upload(("a", None)), "parquet")
    assert not r.has_changes


def test_formato_invalido():
    with pytest.raises(ValueError):
        check(staging("a"), upload("a"), "json")


## Testes sobre partições
def test_particao_igual_nao_gera_achado():
    r = check(
        staging("a", keys=("ano", "mes")),
        upload("a", keys=("ano", "mes")),
        "csv",
    )
    assert not r.has_changes


@pytest.mark.parametrize(
    "st_keys, up_keys",
    [
        (("ano",), ("ano", "mes")),
        (("ano", "mes"), ("mes", "ano")),
        (("ano",), ()),
        ((), ("ano",)),
    ],
)
def test_layout_de_particao_diferente_e_erro(st_keys, up_keys):
    r = check(staging("a", keys=st_keys), upload("a", keys=up_keys), "parquet")
    assert any("layout de partição" in e for e in r.errors)


def test_layout_desconhecido_e_warning_e_nao_erro():
    r = check(staging("a", keys=None), upload("a", keys=("ano",)), "csv")
    assert not r.errors
    assert any("não verificado" in w for w in r.warnings)


## Testes sobre Overwrite
def test_overwrite_rebaixa_erros_a_warning():
    r = check(staging("a", "b"), upload("b", "a"), "csv", overwrite=True)
    assert not r.errors
    assert r.warnings
    r.raise_if_errors()  # não levanta


def test_overwrite_sem_mudancas_nao_avisa():
    r = check(staging("a"), upload("a"), "csv", overwrite=True)
    assert not r.has_changes


def test_overwrite_inclui_nota_de_schema_substituido():
    r = check(staging("a", "b"), upload("a"), "parquet", overwrite=True)
    assert "dbt" in r.warnings[-1]


## Testes sobre Resultados
def test_raise_if_errors_levanta_com_titulo():
    r = check(staging("a", "b"), upload("a"), "parquet")
    with pytest.raises(SchemaMismatchError, match="Meu titulo"):
        r.raise_if_errors("Meu titulo")


def test_render_trunca_mensagem_longa():
    cols = [f"col_{i}" for i in range(2000)]
    r = check(staging("a"), upload("a", *cols), "csv")
    assert len(r.render("t")) <= 2000


## Testes sobre leitura de schemas
def test_upload_csv_le_header_e_chaves_de_particao(tmp_path):
    folder = tmp_path / "ano=2024" / "mes=01"
    folder.mkdir(parents=True)
    pd.DataFrame({"Nome Coluna": ["x"], "b": ["y"]}).to_csv(
        folder / "data.csv", index=False
    )
    s = UploadSchema.from_files(tmp_path, "csv")
    assert [c.name for c in s.columns] == ["Nome_Coluna", "b"]
    assert all(c.data_type is None for c in s.columns)
    assert s.partition_keys == ("ano", "mes")


def test_upload_csv_sem_particao(tmp_path):
    f = tmp_path / "data.csv"
    pd.DataFrame({"a": ["x"]}).to_csv(f, index=False)
    assert UploadSchema.from_files(f, "csv").partition_keys == ()


def test_upload_parquet_le_tipos(tmp_path):
    table = pa.table(
        {
            "i": pa.array([1], pa.int64()),
            "f": pa.array([1.0], pa.float64()),
            "s": pa.array(["x"], pa.string()),
            "b": pa.array([True], pa.bool_()),
        }
    )
    pq.write_table(table, tmp_path / "data.parquet")
    s = UploadSchema.from_files(tmp_path, "parquet")
    assert {c.name: c.data_type for c in s.columns} == {
        "i": "INT64",
        "f": "FLOAT64",
        "s": "STRING",
        "b": "BOOL",
    }


def test_upload_sem_arquivo_levanta(tmp_path):
    with pytest.raises(FileNotFoundError):
        UploadSchema.from_files(tmp_path, "csv")


def _bq_table(fields, definition=None, hive=False):
    table = MagicMock()
    table._properties = (
        {"partitionDefinition": definition} if definition else {}
    )
    table.schema = [MagicMock(field_type=t) for _, t in fields]
    for f, (name, _) in zip(table.schema, fields, strict=False):
        f.name = name
    table.external_data_configuration.hive_partitioning = (
        object() if hive else None
    )
    return table


def test_staging_remove_chaves_de_particao_das_colunas():
    table = _bq_table(
        [("a", "STRING"), ("ano", "STRING"), ("mes", "STRING")],
        definition={"partitionedColumn": [{"field": "ano"}, {"field": "mes"}]},
        hive=True,
    )
    s = StagingSchema.from_table(table)
    assert [c.name for c in s.columns] == ["a"]
    assert s.partition_keys == ("ano", "mes")


def test_staging_sem_particao():
    s = StagingSchema.from_table(_bq_table([("a", "STRING")]))
    assert s.partition_keys == ()


def test_staging_hive_sem_definition_tem_chaves_desconhecidas():
    s = StagingSchema.from_table(_bq_table([("a", "STRING")], hive=True))
    assert s.partition_keys is None


## Retries
def _state_with(error):
    state = MagicMock()
    state.result.return_value = error
    return state


def test_retry_nao_retenta_schema_mismatch():
    state = _state_with(SchemaMismatchError("x"))
    # pyrefly: ignore [bad-argument-type]
    assert _retry_unless_schema_mismatch(None, None, state) is False


def test_retry_retenta_outras_falhas():
    state = _state_with(RuntimeError("bigquery fora do ar"))
    # pyrefly: ignore [bad-argument-type]
    assert _retry_unless_schema_mismatch(None, None, state) is True
