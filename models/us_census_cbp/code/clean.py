"""Clean U.S. Census County Business Patterns (CBP) into wide, year-partitioned parquet.

Three tables (one row per source row):
  national  ← cbp<yy>us.txt   (naics x lfo, per-size emp+payroll+counts)
  state     ← cbp<yy>st.txt   (+ id_state)
  county    ← cbp<yy>co.txt   (id_state,id_county x naics; establishment counts only)

The raw schema shifts across eras (no lfo pre-2008; noise flags 2017+; empflag/fS
1998-2016; smallest band is n<5 in modern files, n1_4 in older) - so every column is
resolved BY HEADER NAME, and columns absent in a given file become null.

Output: data/output/<table>/year=<YYYY>/data.parquet  (year excluded from file; it is
the hive partition). Payroll fields are multiplied by 1000 (source is $1,000s) -> USD.

Usage:
  python clean.py                      # all tables, all years 1998-2023
  python clean.py national             # one table, all years
  python clean.py county 2023          # one table, one year (quick check)
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE = Path(__file__).resolve().parents[1] / "data"
INPUT = BASE / "input"
OUTPUT = BASE / "output"
YEARS = list(range(1998, 2024))

# 9 employment-size bands common to all files
BANDS9 = [
    "1_4",
    "5_9",
    "10_19",
    "20_49",
    "50_99",
    "100_249",
    "250_499",
    "500_999",
    "1000",
]
# county-only finer 1,000+ bands: output name -> source band token
BANDS_CO_EXTRA = {
    "1000_1499": "1000_1",
    "1500_2499": "1000_2",
    "2500_4999": "1000_3",
    "5000_more": "1000_4",
}


def naics_version(year: int) -> str:
    """Return the NAICS vintage CBP used for a given data year.

    CBP NAICS-vintage adoption. Note: CBP still publishes NAICS 2017
    through 2023 (it lags the revision) - 2022/2023 data use NAICS
    2017, NOT NAICS 2022.

    Args:
        year: CBP reference year (1998-2023).

    Returns:
        NAICS vintage as a four-digit year string, e.g. "2012".
    """
    if year <= 2002:
        return "1997"
    if year <= 2007:
        return "2002"
    if year <= 2011:
        return "2007"
    if year <= 2016:
        return "2012"
    return "2017"


def _band_src_tokens(band: str) -> list[str]:
    """Source column band token variants (modern files use '<5' for the 1-4 band).

    Args:
        band: Output band name, e.g. "1_4" or "100_249".

    Returns:
        Source header tokens to try, in preference order.
    """
    return [band, "<5"] if band == "1_4" else [band]


class Cols:
    """Case-insensitive header resolver over one raw CBP file."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Build the lowercase header lookup for one raw file.

        Args:
            df: Raw CBP file whose headers are resolved by name.
        """
        self.df = df
        self.lut = {c.lower(): c for c in df.columns}

    def get(self, *names: str) -> pd.Series | None:
        """Return the first column matching any of the given names.

        Args:
            *names: Candidate header names, tried in order.

        Returns:
            The matching column, or None if no name is present in the
            file.
        """
        for n in names:
            c = self.lut.get(n.lower())
            if c is not None:
                return self.df[c]
        return None

    def band(
        self, prefix: str, band: str, suffix: str = ""
    ) -> pd.Series | None:
        """Return the column holding one employment-size band.

        Args:
            prefix: Measure prefix, e.g. "n", "e", "f", "q" or "a".
            band: Output band name, e.g. "1_4".
            suffix: Trailing marker, e.g. "nf" for noise flags.

        Returns:
            The matching column, or None if absent in this file.
        """
        cols = [f"{prefix}{tok}{suffix}" for tok in _band_src_tokens(band)]
        return self.get(*cols)


def _int(series: pd.Series | None) -> pd.Series:
    """Coerce a source column to nullable Int64.

    Args:
        series: Source column, or None when absent from the file.

    Returns:
        Int64 series; an NA series when the input is None.
    """
    if series is None:
        return pd.Series(pd.NA, dtype="Int64")
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _int_x1000(series: pd.Series | None) -> pd.Series:
    """Coerce a payroll column published in $1,000s to Int64 USD.

    Args:
        series: Source payroll column in $1,000s, or None if absent.

    Returns:
        Int64 series in USD; an NA series when the input is None.
    """
    if series is None:
        return pd.Series(pd.NA, dtype="Int64")
    return (
        (pd.to_numeric(series, errors="coerce") * 1000).round().astype("Int64")
    )


def _str(series: pd.Series | None) -> pd.Series:
    """Coerce a source column to stripped strings, blank -> NA.

    Args:
        series: Source column, or None when absent from the file.

    Returns:
        String series with empty values masked to NA; an NA series
        when the input is None.
    """
    if series is None:
        return pd.Series(pd.NA, dtype="object")
    s = series.astype("string").str.strip()
    return s.mask(s == "", pd.NA)


def _normalize_naics(series: pd.Series) -> pd.Series:
    """Strip CBP aggregation fill to the native code; total ('------') -> NA (dropped).

    Args:
        series: Raw naics column from a CBP file.

    Returns:
        String series of 2-6 digit NAICS codes, NA on aggregate-total
        rows.
    """
    core = (
        series.astype("string")
        .str.strip()
        .str.replace("-", "", regex=False)
        .str.replace("/", "", regex=False)
    )
    return core.mask(~core.str.fullmatch(r"\d{2,6}").fillna(False), pd.NA)


def _read(path: Path) -> pd.DataFrame:
    """Read one raw CBP text file as strings with lowercase headers.

    Args:
        path: Path to a cbp<yy><level>.txt source file.

    Returns:
        All-string DataFrame with stripped, lowercased column names.
    """
    df = pd.read_csv(
        path, dtype=str, keep_default_na=False, encoding="latin-1"
    )
    df.columns = [c.strip().strip('"').lower() for c in df.columns]
    return df


# ----- per-table builders -------------------------------------------------


def _totals(c: Cols, out: pd.DataFrame) -> None:
    """Add the all-size totals and their disclosure flags.

    Args:
        c: Header resolver over the raw file.
        out: Output frame, mutated in place.
    """
    out["establishments"] = _int(c.get("est"))
    out["employment"] = _int(c.get("emp"))
    out["employment_flag"] = _str(c.get("empflag"))
    out["employment_noise_flag"] = _str(c.get("emp_nf"))
    out["payroll_first_quarter"] = _int_x1000(c.get("qp1"))
    out["payroll_first_quarter_noise_flag"] = _str(c.get("qp1_nf"))
    out["payroll_annual"] = _int_x1000(c.get("ap"))
    out["payroll_annual_noise_flag"] = _str(c.get("ap_nf"))


def _band_measures(c: Cols, out: pd.DataFrame) -> None:
    """national/state: 8 columns per band (values + flags).

    Args:
        c: Header resolver over the raw file.
        out: Output frame, mutated in place.
    """
    for b in BANDS9:
        out[f"establishments_{b}"] = _int(c.band("n", b))
        out[f"employment_{b}"] = _int(c.band("e", b))
        out[f"employment_{b}_flag"] = _str(c.band("f", b))
        out[f"employment_{b}_noise_flag"] = _str(c.band("e", b, "nf"))
        out[f"payroll_first_quarter_{b}"] = _int_x1000(c.band("q", b))
        out[f"payroll_first_quarter_{b}_noise_flag"] = _str(
            c.band("q", b, "nf")
        )
        out[f"payroll_annual_{b}"] = _int_x1000(c.band("a", b))
        out[f"payroll_annual_{b}_noise_flag"] = _str(c.band("a", b, "nf"))


def build_national(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Build the national table from one cbp<yy>us.txt file.

    Args:
        df: Raw national file as returned by _read.
        year: CBP reference year.

    Returns:
        One row per naics x lfo, aggregate-total rows dropped.
    """
    c = Cols(df)
    out = pd.DataFrame()
    out["naics"] = _normalize_naics(c.get("naics"))
    out["naics_version"] = naics_version(year)
    lfo = c.get("lfo")
    out["lfo"] = _str(lfo) if lfo is not None else "-"
    _totals(c, out)
    _band_measures(c, out)
    return out[out["naics"].notna()].reset_index(drop=True)


def build_state(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Build the state table from one cbp<yy>st.txt file.

    Args:
        df: Raw state file as returned by _read.
        year: CBP reference year.

    Returns:
        One row per id_state x naics x lfo, total rows dropped.
    """
    c = Cols(df)
    out = pd.DataFrame()
    out["id_state"] = _str(c.get("fipstate")).str.zfill(2)
    out["naics"] = _normalize_naics(c.get("naics"))
    out["naics_version"] = naics_version(year)
    lfo = c.get("lfo")
    out["lfo"] = _str(lfo) if lfo is not None else "-"
    _totals(c, out)
    _band_measures(c, out)
    return out[out["naics"].notna()].reset_index(drop=True)


def build_county(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Build the county table from one cbp<yy>co.txt file.

    Args:
        df: Raw county file as returned by _read.
        year: CBP reference year.

    Returns:
        One row per id_state x id_county x naics, totals dropped.
    """
    c = Cols(df)
    out = pd.DataFrame()
    st = _str(c.get("fipstate")).str.zfill(2)
    cty = _str(c.get("fipscty")).str.zfill(3)
    out["id_state"] = st
    out["id_county"] = st + cty
    out["naics"] = _normalize_naics(c.get("naics"))
    out["naics_version"] = naics_version(year)
    _totals(c, out)
    for b in BANDS9:
        out[f"establishments_{b}"] = _int(c.band("n", b))
    for outname, tok in BANDS_CO_EXTRA.items():
        out[f"establishments_{outname}"] = _int(c.get(f"n{tok}"))
    return out[out["naics"].notna()].reset_index(drop=True)


# ----- schema (column order + types; year is the hive partition, not in file) ----


def _schema(table: str) -> pa.Schema:
    """Build the output Arrow schema for one table.

    Args:
        table: Table name - "national", "state" or "county".

    Returns:
        Arrow schema in output column order; year is the hive
        partition and is not a field.

    Raises:
        KeyError: If table is not one of the three known names.
    """
    f = [
        ("id_state", pa.string()),
        ("id_county", pa.string()),
        ("naics", pa.string()),
        ("naics_version", pa.string()),
        ("lfo", pa.string()),
    ]
    keep = {
        "national": ["naics", "naics_version", "lfo"],
        "state": ["id_state", "naics", "naics_version", "lfo"],
        "county": ["id_state", "id_county", "naics", "naics_version"],
    }[table]
    fields = [(n, t) for n, t in f if n in keep]
    fields += [
        ("establishments", pa.int64()),
        ("employment", pa.int64()),
        ("employment_flag", pa.string()),
        ("employment_noise_flag", pa.string()),
        ("payroll_first_quarter", pa.int64()),
        ("payroll_first_quarter_noise_flag", pa.string()),
        ("payroll_annual", pa.int64()),
        ("payroll_annual_noise_flag", pa.string()),
    ]
    if table in ("national", "state"):
        for b in BANDS9:
            fields += [
                (f"establishments_{b}", pa.int64()),
                (f"employment_{b}", pa.int64()),
                (f"employment_{b}_flag", pa.string()),
                (f"employment_{b}_noise_flag", pa.string()),
                (f"payroll_first_quarter_{b}", pa.int64()),
                (f"payroll_first_quarter_{b}_noise_flag", pa.string()),
                (f"payroll_annual_{b}", pa.int64()),
                (f"payroll_annual_{b}_noise_flag", pa.string()),
            ]
    else:
        for b in list(BANDS9) + list(BANDS_CO_EXTRA):
            fields.append((f"establishments_{b}", pa.int64()))
    return pa.schema(fields)


BUILDERS = {
    "national": ("us", build_national),
    "state": ("st", build_state),
    "county": ("co", build_county),
}


def _src_path(level_suffix: str, year: int) -> Path:
    """Build the raw input path for one geographic level and year.

    Args:
        level_suffix: Source level token - "us", "st" or "co".
        year: CBP reference year.

    Returns:
        Path to the cbp<yy><level_suffix>.txt file under input/.
    """
    yy = f"{year % 100:02d}"
    return INPUT / f"cbp{yy}{level_suffix}.txt"


def clean_one(table: str, year: int) -> pd.DataFrame | None:
    """Clean one table-year and write its parquet partition.

    Args:
        table: Table name - "national", "state" or "county".
        year: CBP reference year.

    Returns:
        The cleaned frame, or None if the source file is missing.
    """
    suffix, builder = BUILDERS[table]
    path = _src_path(suffix, year)
    if not path.exists():
        print(f"  [skip] {table} {year}: missing {path.name}")
        return None
    df = _read(path)
    out = builder(df, year)
    # Some source files repeat records verbatim (e.g. 1999 county file duplicates a
    # block for FIPS 01045 across health-care NAICS). Identical rows carry no extra
    # information - drop them. Rows that share a key but differ in values are kept
    # (faithful to source); the dbt uniqueness test allows that tiny proportion.
    before = len(out)
    out = out.drop_duplicates().reset_index(drop=True)
    if len(out) < before:
        print(
            f"  [dedupe] {table} {year}: dropped {before - len(out)} exact-duplicate row(s)"
        )
    schema = _schema(table)
    out = out[[f.name for f in schema]]
    for f in schema:
        if f.type == pa.int64():
            out[f.name] = out[f.name].astype("Int64")
        else:
            out[f.name] = out[f.name].astype("string")
    table_pa = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    dest = OUTPUT / table / f"year={year}"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(table_pa, dest / "data.parquet", compression="snappy")
    return out


def main() -> None:
    """Clean the tables and years named on the command line.

    With no arguments, cleans every table for every year in YEARS.
    """
    args = sys.argv[1:]
    tables = [a for a in args if a in BUILDERS] or list(BUILDERS)
    years = [int(a) for a in args if a.isdigit()] or YEARS
    for table in tables:
        total = 0
        for year in years:
            out = clean_one(table, year)
            if out is not None:
                total += len(out)
                print(f"  {table} {year}: {len(out):,} rows")
        print(f"== {table}: {total:,} rows over {len(years)} years ==")


if __name__ == "__main__":
    main()
