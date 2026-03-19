"""
Shared helper functions for the br_tse_eleicoes pipeline.
Mirrors patterns from the Stata .do files.
"""

import re
from pathlib import Path

import pandas as pd
from config import MUNICIPIO_DIR_CSV, NULL_SENTINELS

# ---------------------------------------------------------------------------
# Reading raw TSE files
# ---------------------------------------------------------------------------


def read_raw_csv(
    filepath_pattern: str,
    *,
    drop_first_row: bool = True,
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Read a raw TSE semicolon-delimited file.

    Tries .txt first, then .csv — matching Stata's `cap import delimited` pattern.
    All columns are read as strings (stringcols(_all)).
    No header row (varn(nonames)); positional column names v1, v2, ... .

    Parameters
    ----------
    filepath_pattern : str
        Path WITHOUT extension. E.g. "input/consulta_cand/consulta_cand_2020/consulta_cand_2020_SP"
        Will try .txt then .csv.
    drop_first_row : bool
        If True, drop the first row (Stata: `drop in 1`). Many TSE files have a
        header row that Stata skips this way.
    encoding : str
        File encoding. Default "utf-8".
    """
    base = Path(filepath_pattern)
    txt_path = base.with_suffix(".txt")
    csv_path = base.with_suffix(".csv")

    path = None
    if txt_path.exists():
        path = txt_path
    elif csv_path.exists():
        path = csv_path
    else:
        raise FileNotFoundError(f"Neither {txt_path} nor {csv_path} found.")

    df = pd.read_csv(
        path,
        sep=";",
        header=None,
        dtype=str,
        encoding=encoding,
        quotechar='"',
        keep_default_na=False,
        on_bad_lines="warn",
    )

    # Stata-style positional column names: v1, v2, ...
    df.columns = [f"v{i + 1}" for i in range(len(df.columns))]

    if drop_first_row and len(df) > 0:
        df = df.iloc[1:].reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# Null / sentinel cleaning
# ---------------------------------------------------------------------------


def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace TSE null sentinel values with empty string across all columns.
    Mirrors the Stata `foreach k of varlist _all { replace ... }` block.
    """
    return df.replace(
        {col: {v: "" for v in NULL_SENTINELS} for col in df.columns}
    )


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def parse_date_br(s: pd.Series) -> pd.Series:
    """
    Convert DD/MM/YYYY → YYYY-MM-DD.
    Returns empty string for malformed or pre-1900 dates.
    Mirrors Stata:
        replace data = substr(data, 7, 4) + "-" + substr(data, 4, 2) + "-" + substr(data, 1, 2)
        replace data = "" if real(substr(data, 1, 4)) < 1900
    """

    def _convert(val):
        if not isinstance(val, str) or len(val) < 10:
            return ""
        # Try DD/MM/YYYY
        match = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", val.strip())
        if not match:
            return val  # leave as-is if not matching pattern
        day, month, year = match.groups()
        if int(year) < 1900:
            return ""
        return f"{year}-{month}-{day}"

    return s.map(_convert)


# ---------------------------------------------------------------------------
# Padding functions
# ---------------------------------------------------------------------------


def pad_cpf(s: pd.Series) -> pd.Series:
    """Left-pad CPF to 11 digits with zeros. Empty/null stays empty."""

    def _pad(val):
        if not val or not val.strip():
            return ""
        val = val.strip()
        if val in ("0", "00000000000"):
            return ""
        return val.zfill(11)

    return s.map(_pad)


def pad_titulo(s: pd.Series) -> pd.Series:
    """Left-pad título eleitoral to 12 digits with zeros. Empty/null stays empty."""

    def _pad(val):
        if not val or not val.strip():
            return ""
        val = val.strip()
        if val in ("0", "000000000000"):
            return ""
        return val.zfill(12)

    return s.map(_pad)


# ---------------------------------------------------------------------------
# Municipality directory merge
# ---------------------------------------------------------------------------

_MUNICIPIO_CACHE = None


def load_municipio_directory() -> pd.DataFrame:
    """Load the municipality directory (id_municipio ↔ id_municipio_tse)."""
    global _MUNICIPIO_CACHE
    if _MUNICIPIO_CACHE is None:
        df = pd.read_csv(MUNICIPIO_DIR_CSV, encoding="utf-8", dtype=str)
        _MUNICIPIO_CACHE = df[["id_municipio", "id_municipio_tse"]].copy()
    return _MUNICIPIO_CACHE


def merge_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join on id_municipio_tse to add id_municipio.
    Mirrors Stata: merge m:1 id_municipio_tse using `municipio' / drop if _merge == 2 / drop _merge
    """
    mun = load_municipio_directory()
    merged = df.merge(mun, on="id_municipio_tse", how="left")
    return merged


# ---------------------------------------------------------------------------
# Partitioned CSV output
# ---------------------------------------------------------------------------


def save_partitioned(
    df: pd.DataFrame,
    table_name: str,
    partition_cols: list[str],
    output_dir: Path,
):
    """
    Write Hive-style partitioned CSVs.

    Creates directory structure like:
        output_dir/table_name/ano=2020/sigla_uf=SP/table_name.csv

    Partition columns are removed from the CSV files (they live in the path).
    """
    if df.empty:
        return

    data_cols = [c for c in df.columns if c not in partition_cols]

    for keys, group in df.groupby(partition_cols, sort=True):
        if isinstance(keys, str):
            keys = (keys,)

        parts = "/".join(
            f"{col}={val}"
            for col, val in zip(partition_cols, keys, strict=True)
        )
        dest = output_dir / table_name / parts
        dest.mkdir(parents=True, exist_ok=True)

        group[data_cols].to_csv(
            dest / f"{table_name}.csv",
            index=False,
            encoding="utf-8",
        )
