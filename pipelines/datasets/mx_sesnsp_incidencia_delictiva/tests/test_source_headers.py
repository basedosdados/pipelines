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
    normalize_headers,
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
