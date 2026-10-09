"""Regression tests for the SESNSP source-header reader.

The 2026-08 release broke the armed pipeline with ``KeyError: 'Año'``. The
source is behind an Imperva challenge, so the exact change could not be
observed; these tests pin the two plausible causes — an encoding flip and a
header re-spelling — plus the diagnosable failure when it is neither.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.utils import (
    _canonical_key,
    _read_source_csv,
    melt_wide,
    normalize_headers,
    trim_trailing_empty_months,
)

HEADER = (
    "Año,Clave_Ent,Entidad,Bien jurídico afectado,Tipo de delito,"
    "Subtipo de delito,Modalidad,Enero,Febrero"
)
ROW = "2026,01,Aguascalientes,La vida,Homicidio,Homicidio doloso,Con arma,3,5"


def _write(tmp_path, text: str, encoding: str):
    path = tmp_path / "RNID-sample.csv"
    path.write_bytes(f"{text}\n".encode(encoding))
    return path


@pytest.mark.parametrize("encoding", ["latin-1", "utf-8"])
def test_reads_either_encoding(tmp_path, encoding):
    """A latin-1 or utf-8 export both resolve the accented year column."""
    csv_path = _write(tmp_path, f"{HEADER}\n{ROW}", encoding)

    df = _read_source_csv(csv_path)

    assert "Año" in df.columns
    assert "Bien jurídico afectado" in df.columns
    assert df["Año"].tolist() == ["2026"]


def test_utf8_is_not_silently_mojibaked(tmp_path):
    """utf-8 is tried first, so its accents survive rather than becoming 'AÃ±o'."""
    csv_path = _write(tmp_path, f"{HEADER}\n{ROW}", "utf-8")

    df = _read_source_csv(csv_path)

    assert not any("Ã" in c for c in df.columns)


@pytest.mark.parametrize(
    "raw, canonical",
    [
        ("AÑO", "Año"),
        ("anio", "Año"),
        ("  Año  ", "Año"),
        ("BIEN JURIDICO AFECTADO", "Bien jurídico afectado"),
        ("Cve Municipio", "Cve. Municipio"),
        ("Rango de Edad", "Rango de edad"),
    ],
)
def test_header_respelling_is_absorbed(raw, canonical):
    """Case, accent and punctuation drift in the header resolves to canonical."""
    df = pd.DataFrame({raw: ["2026"]})

    assert canonical in normalize_headers(df).columns


def test_unresolvable_header_names_the_actual_columns(tmp_path):
    """A genuinely new header fails with the decoded columns, not a KeyError."""
    csv_path = _write(tmp_path, "period,state,count\n2026,01,3", "utf-8")

    with pytest.raises(ValueError, match="no year column"):
        _read_source_csv(csv_path)

    with pytest.raises(ValueError, match="period"):
        _read_source_csv(csv_path)


def test_canonical_key_collapses_punctuation_and_accents():
    assert _canonical_key("Cve. Municipio") == "cvemunicipio"
    assert _canonical_key("Año") == _canonical_key("AÑO") == "ano"
    assert _canonical_key("anio") == "anio"  # alias, not a normalization


# --------------------------------------------------------------- future periods
def _wide_row(year: str, **months) -> pd.DataFrame:
    """One wide source row for `year`, with the named months filled."""
    base = {
        "Año": [year],
        "Clave_Ent": ["01"],
        "Bien jurídico afectado": ["La vida"],
        "Tipo de delito": ["Homicidio"],
        "Subtipo de delito": ["Homicidio doloso"],
        "Modalidad": ["Con arma"],
    }
    base.update({m: [v] for m, v in months.items()})
    return pd.DataFrame(base)


def test_months_after_max_period_are_dropped():
    """The 2026-08 release pads the rest of the year with explicit 0.

    Blank-means-unpublished no longer holds, so a zero for a month that has not
    happened is padding and must not count as coverage.
    """
    df = _wide_row("2026", Enero="3", Junio="5", Agosto="0", Diciembre="0")

    long = melt_wide(df, muni=False, victimas=False, max_period=(2026, 6))

    assert sorted(long["mes"].tolist()) == [1, 6]
    assert long["ano"].unique().tolist() == [2026]


def test_the_boundary_month_is_kept():
    """`max_period` is inclusive — the latest published month is real data."""
    df = _wide_row("2026", Junio="5")

    long = melt_wide(df, muni=False, victimas=False, max_period=(2026, 6))

    assert long["mes"].tolist() == [6]


def test_past_years_are_untouched_including_explicit_zeros():
    """A zero in a past year is a real count of zero, not padding."""
    df = _wide_row("2019", Enero="0", Diciembre="0")

    long = melt_wide(df, muni=False, victimas=False, max_period=(2026, 6))

    assert sorted(long["mes"].tolist()) == [1, 12]
    assert long["cantidad"].tolist() == [0, 0]


def test_blank_months_are_still_dropped():
    """The original guard survives — blank is still unpublished."""
    df = (
        _wide_row("2026", Enero="3", Fevereiro_unused="")
        if False
        else _wide_row("2026", Enero="3", Marzo="")
    )

    long = melt_wide(df, muni=False, victimas=False, max_period=(2026, 6))

    assert long["mes"].tolist() == [1]


def test_defaults_to_the_current_month(monkeypatch):
    """Without an explicit cutoff the transform uses today."""
    df = _wide_row("2099", Enero="7")

    long = melt_wide(df, muni=False, victimas=False)

    assert long.empty


# ------------------------------------------------------- trailing zero padding
def test_trailing_all_zero_months_are_dropped():
    """The measured 2026-08 shape: real data to July, zeros to December."""
    long = pd.DataFrame(
        {
            "mes": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "cantidad": [10, 10, 10, 10, 10, 10, 10, 0, 0, 0, 0, 0],
        }
    )

    trimmed = trim_trailing_empty_months(long, "estatal_delitos", 2026)

    assert trimmed["mes"].max() == 7
    assert len(trimmed) == 7


def test_a_zero_inside_the_series_is_kept():
    """Only *trailing* zeros are padding; an interior zero is a real count."""
    long = pd.DataFrame({"mes": [1, 2, 3, 4], "cantidad": [10, 0, 10, 0]})

    trimmed = trim_trailing_empty_months(long, "t", 2026)

    assert trimmed["mes"].tolist() == [1, 2, 3]


def test_a_fully_zero_year_yields_nothing():
    long = pd.DataFrame({"mes": [1, 2], "cantidad": [0, 0]})

    assert trim_trailing_empty_months(long, "t", 2027).empty


def test_a_year_with_no_padding_is_untouched():
    long = pd.DataFrame({"mes": [1, 2, 3], "cantidad": [5, 5, 5]})

    assert len(trim_trailing_empty_months(long, "t", 2025)) == 3
