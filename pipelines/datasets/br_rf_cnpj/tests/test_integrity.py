"""Testes dos controles de integridade do flow br_rf_cnpj.

Sem rede e sem GCP: o CSV de entrada é sintético e os downloads são mockados.
"""

import datetime
from pathlib import Path

import pandas as pd
import pytest

from pipelines.datasets.br_rf_cnpj import tasks
from pipelines.datasets.br_rf_cnpj.constants import (
    COLUNAS_ESTABELECIMENTO,
    EXPECTED_FILES_PER_TABLE,
)
from pipelines.datasets.br_rf_cnpj.utils import (
    process_csv_estabelecimentos,
    validate_expected_file_count,
    validate_partition_rows,
)

DATA_REF = "2026-01"
DATA_COLETA = "2026-01-15"


# --------------------------------------------------------------------------
# validate_expected_file_count
# --------------------------------------------------------------------------
@pytest.mark.parametrize("table", ["empresas", "estabelecimentos", "socios"])
def test_file_count_expected_passes(table):
    validate_expected_file_count(table, 10, EXPECTED_FILES_PER_TABLE[table], 0)


@pytest.mark.parametrize("found", [0, 9, 11])
def test_file_count_mismatch_raises(found):
    with pytest.raises(ValueError, match="socios"):
        validate_expected_file_count("socios", found, 10, 0)


def test_file_count_tolerance():
    validate_expected_file_count("socios", 9, 10, tolerance=1)
    with pytest.raises(ValueError):
        validate_expected_file_count("socios", 8, 10, tolerance=1)


# --------------------------------------------------------------------------
# validate_partition_rows
# --------------------------------------------------------------------------
def test_rows_equal_passes():
    validate_partition_rows(100, 100, tolerance=0.0, arquivo="a.csv")


def test_rows_lost_raises_with_zero_tolerance():
    with pytest.raises(ValueError, match=r"a\.csv"):
        validate_partition_rows(100, 99, tolerance=0.0, arquivo="a.csv")


def test_rows_lost_within_tolerance_only_warns(monkeypatch):
    avisos = []
    monkeypatch.setattr(
        "pipelines.datasets.br_rf_cnpj.utils.log",
        lambda msg, level="info": avisos.append((level, msg)),
    )
    validate_partition_rows(100, 99, tolerance=0.05, arquivo="a.csv")
    assert avisos and avisos[0][0] == "warning"


def test_rows_lost_above_tolerance_raises():
    with pytest.raises(ValueError):
        validate_partition_rows(100, 90, tolerance=0.05)


def test_rows_zero_input_does_not_divide_by_zero():
    # Documenta o comportamento atual: CSV vazio passa em silêncio.
    validate_partition_rows(0, 0)


# --------------------------------------------------------------------------
# process_csv_estabelecimentos (CSV sintético)
# --------------------------------------------------------------------------
def _linha(basico: str, uf: str) -> dict:
    row = dict.fromkeys(COLUNAS_ESTABELECIMENTO, "")
    row.update(
        cnpj_basico=basico,
        cnpj_ordem="1",
        cnpj_dv="1",
        data_situacao_cadastral="20200131",
        data_inicio_atividade="20100101",
        sigla_uf=uf,
    )
    return row


def _escreve_input(input_dir: Path, ufs: list[str]) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([_linha(str(i), uf) for i, uf in enumerate(ufs)])
    df.to_csv(
        input_dir / "K3241.K03200Y0.D60110.ESTABELE",
        sep=";",
        header=False,
        index=False,
        encoding="latin1",
    )


def _linhas_gravadas(output: Path) -> int:
    return sum(
        len(pd.read_csv(f, dtype=str))
        for f in output.glob(
            "data_referencia=*/sigla_uf=*/estabelecimentos_*.csv"
        )
    )


def _roda(tmp_path: Path, ufs: list[str]) -> tuple[dict, Path]:
    inp, out = tmp_path / "in", tmp_path / "out"
    _escreve_input(inp, ufs)
    stats = process_csv_estabelecimentos(
        inp, out, DATA_REF, DATA_COLETA, 0, chunk_size=2
    )
    return stats, out


def test_estabelecimentos_all_valid_ufs(tmp_path):
    stats, out = _roda(tmp_path, ["SP", "SP", "RJ", "MG", "AC"])
    assert stats == {"rows_in": 5, "rows_out": 5}
    assert _linhas_gravadas(out) == 5
    validate_partition_rows(**stats, tolerance=0.0)


def test_estabelecimentos_invalid_uf_is_detected(tmp_path):
    stats, out = _roda(tmp_path, ["SP", "ZZ", "RJ", "ZZ"])
    assert stats["rows_in"] == 4
    assert stats["rows_out"] == 2
    assert _linhas_gravadas(out) == 2
    with pytest.raises(ValueError, match="2 de 4"):
        validate_partition_rows(**stats, tolerance=0.0)


def test_estabelecimentos_retry_does_not_duplicate(tmp_path):
    inp, out = tmp_path / "in", tmp_path / "out"
    ufs = ["SP", "RJ", "SP"]
    for _ in range(2):
        _escreve_input(inp, ufs)  # o processamento remove o input
        process_csv_estabelecimentos(inp, out, DATA_REF, DATA_COLETA, 0)
    assert _linhas_gravadas(out) == 3


# --------------------------------------------------------------------------
# main(): a contagem de arquivos aborta antes de qualquer download
# --------------------------------------------------------------------------
def test_main_aborts_before_download_on_wrong_file_count(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    arquivos = [
        (f"Estabelecimentos{i}.zip", f"http://x/{i}") for i in range(9)
    ]
    monkeypatch.setattr(tasks, "get_table_files", lambda *a, **k: arquivos)

    def _nao_deveria_baixar(*a, **k):
        raise AssertionError("download chamado apesar da contagem inválida")

    monkeypatch.setattr(tasks, "download_unzip_csv", _nao_deveria_baixar)

    with pytest.raises(ValueError, match="estabelecimentos"):
        tasks.main.fn(
            tables=["estabelecimentos"],
            folder_date=DATA_REF,
            last_modified_date=datetime.date(2026, 1, 15),
        )
