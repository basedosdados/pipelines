"""Seleção das UF-mês a recarregar no SIH (pipelines.crawler.datasus)."""

from datetime import UTC, date, datetime

import pytest

from pipelines.crawler.datasus.tasks import parse_year_months
from pipelines.crawler.datasus.utils import (
    DATASUS_FTP_TZ,
    months_window,
    parse_datasus_list_line,
    select_partitions_to_reprocess,
)

DIR = "dissemin/publicos/SIHSUS/200801_/Dados"


def brt(*args):
    return datetime(*args, tzinfo=DATASUS_FTP_TZ)


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_parse_list_line_reads_brazilian_time():
    parsed = parse_datasus_list_line(
        "02-08-26  02:17PM              5890232 RDRJ2501.dbc"
    )
    assert parsed is not None
    name, modified = parsed
    assert name == "RDRJ2501.dbc"
    assert modified == brt(2026, 2, 8, 14, 17)
    assert parse_datasus_list_line("01-08-18  03:38PM  <DIR>  sub") is None
    assert parse_datasus_list_line("total 3") is None


def test_months_window_spans_year_boundary_and_next_month():
    assert months_window(date(2026, 1, 1), 3) == [
        "2511",
        "2512",
        "2601",
        "2602",
    ]
    assert months_window(date(2025, 12, 1), 1) == ["2512", "2601"]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2507", {"2507"}),
        ("202507", {"2507"}),
        ("2025-07", {"2507"}),
        ("2403 2404", {"2403", "2404"}),
        ("2024-03,2024-04, 2405", {"2403", "2404", "2405"}),
    ],
)
def test_parse_year_months(value, expected):
    assert parse_year_months(value) == expected


@pytest.mark.parametrize("value", ["2513", "abc", "24-3", "2403 99"])
def test_parse_year_months_rejects_bad_months(value):
    with pytest.raises(ValueError):
        parse_year_months(value)


def test_selects_missing_and_rewritten_partitions_only():
    ftp = {
        # carregado em 2025-03, reescrito pelo DATASUS em 2026-02: recarregar
        f"{DIR}/RDRJ2501.dbc": brt(2026, 2, 8, 14, 17),
        # UF publicada depois que o mês foi carregado: nunca entrou na staging
        f"{DIR}/RDAC2501.dbc": brt(2026, 2, 8, 14, 18),
        # sem mudança desde a carga
        f"{DIR}/RDSP2501.dbc": brt(2025, 3, 1, 10, 0),
        # fora da janela
        f"{DIR}/RDRJ2301.dbc": brt(2024, 1, 1, 10, 0),
    }
    staging = {
        "ano=2025/mes=1/sigla_uf=RJ": utc(2025, 3, 12, 9, 37),
        "ano=2025/mes=1/sigla_uf=SP": utc(2025, 3, 12, 9, 37),
    }
    assert select_partitions_to_reprocess(
        ftp, staging, ["2501"], max_months=6
    ) == [
        f"{DIR}/RDAC2501.dbc",
        f"{DIR}/RDRJ2501.dbc",
    ]


def test_compares_ftp_time_in_brazil_against_utc_blobs():
    # 10:00 BRT = 13:00 UTC: o blob das 12:00 UTC é anterior ao arquivo
    ftp = {f"{DIR}/RDRJ2501.dbc": brt(2026, 2, 8, 10, 0)}
    older = {"ano=2025/mes=1/sigla_uf=RJ": utc(2026, 2, 8, 12, 0)}
    newer = {"ano=2025/mes=1/sigla_uf=RJ": utc(2026, 2, 8, 14, 0)}
    assert select_partitions_to_reprocess(ftp, older, ["2501"], 6) == [
        f"{DIR}/RDRJ2501.dbc"
    ]
    assert select_partitions_to_reprocess(ftp, newer, ["2501"], 6) == []


def test_reloads_every_file_of_a_split_partition():
    # Recarregar um pedaço apagaria os outros da partição: vão todos juntos.
    ftp = {
        f"{DIR}/RDSP2501a.dbc": brt(2026, 2, 8, 14, 0),
        f"{DIR}/RDSP2501b.dbc": brt(2025, 3, 1, 10, 0),
    }
    staging = {"ano=2025/mes=1/sigla_uf=SP": utc(2025, 3, 12, 9, 37)}
    assert select_partitions_to_reprocess(ftp, staging, ["2501"], 6) == [
        f"{DIR}/RDSP2501a.dbc",
        f"{DIR}/RDSP2501b.dbc",
    ]


def test_caps_the_number_of_competences_oldest_first():
    ftp = {
        f"{DIR}/RDRJ{yymm}.dbc": brt(2026, 10, 16, 17, 30)
        for yymm in ["2412", "2501", "2502", "2503"]
    }
    selected = select_partitions_to_reprocess(
        ftp, {}, ["2412", "2501", "2502", "2503"], max_months=2
    )
    assert selected == [f"{DIR}/RDRJ2412.dbc", f"{DIR}/RDRJ2501.dbc"]
