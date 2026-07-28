"""Build the us_census_cbp dicionario table (coded value -> label) as parquet.

Covers *every* column the architecture sheets mark `covered_by_dictionary=yes`,
including the per-size-band flag columns, whose code sets are supersets of the
total-level ones (see BAND_EMPFLAG / BAND_NOISE). The covered columns are read
back from `sheet_<table>.tsv` rather than listed by hand, so the dictionary
cannot drift from the architecture. Codes come from CBP record layouts (LFO,
EMPFLAG, noise flags) and the NAICS-vintage mapping.
"""

import csv
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
OUTPUT = Path(__file__).resolve().parents[1] / "data" / "output"
TABLES = ["national", "state", "county"]

# CBP uses NAICS 2017 through 2023 (no NAICS 2022 vintage in this data). Each code
# is in force for a specific span of reference years, mirroring naics_version() in
# clean.py, so the dictionary rows carry per-code coverage rather than the table's.
NAICS_VERSION = {
    "1997": ("NAICS 1997", "1998(1)2002"),
    "2002": ("NAICS 2002", "2003(1)2007"),
    "2007": ("NAICS 2007", "2008(1)2011"),
    "2012": ("NAICS 2012", "2012(1)2016"),
    "2017": ("NAICS 2017", "2017(1)2023"),
}
LFO = {
    "-": "All establishments",
    "C": "C-corporations and other corporate legal forms",
    "Z": "S-corporations",
    "S": "Sole proprietorships",
    "P": "Partnerships",
    "N": "Non-profits",
    "G": "Government",
    "O": "Other",
}
EMPFLAG = {
    "A": "0 to 19 employees",
    "B": "20 to 99 employees",
    "C": "100 to 249 employees",
    "E": "250 to 499 employees",
    "F": "500 to 999 employees",
    "G": "1,000 to 2,499 employees",
    "H": "2,500 to 4,999 employees",
    "I": "5,000 to 9,999 employees",
    "J": "10,000 to 24,999 employees",
    "K": "25,000 to 49,999 employees",
    "L": "50,000 to 99,999 employees",
    "M": "100,000 or more employees",
    "r": "Revised data",
    "S": "Withheld because the estimate did not meet publication standards; employment or payroll set to zero",
}
NOISE = {
    "D": "Withheld to avoid disclosing data for individual companies; the value is included in higher-level totals",
    "G": "Low noise, 0 to less than 2 percent",
    "H": "Medium noise, 2 to less than 5 percent",
    "J": "High noise, 5 percent or more",
    "S": "Withheld because the estimate did not meet publication standards",
}
# The per-size-band flags carry two disclosure codes on top of their total-level
# code set. Verified by taking the union of observed values over the full
# 1998-2023 history of every band column: the band suppression flags run
# A-M/r/S plus D and N, and the band noise flags run D/G/H/J/S plus N, whereas
# the total-level columns never take D or N respectively.
NOT_AVAILABLE = "Not available or not comparable"
BAND_EMPFLAG = {**EMPFLAG, **{"D": NOISE["D"], "N": NOT_AVAILABLE}}
BAND_NOISE = {**NOISE, **{"N": NOT_AVAILABLE}}
# The three all-size measures; every other flag column belongs to a size band.
TOTAL_NOISE_COLUMNS = {
    "employment_noise_flag",
    "payroll_first_quarter_noise_flag",
    "payroll_annual_noise_flag",
}


def code_map(column: str) -> tuple[dict[str, str], str] | None:
    """Return the (code -> label, temporal coverage) pair for a coded column.

    Columns are matched by suffix rather than enumerated, so the per-size-band
    flags are covered automatically. The four flag families differ: the
    total-level ``employment_flag`` carries the A-M size-range codes, the
    total-level ``*_noise_flag`` columns the noise-infusion codes, and the two
    per-band families each add the disclosure codes on top.

    Args:
        column: BigQuery column name from the architecture sheet.

    Returns:
        A ``(codes, coverage)`` pair, or ``None`` if the column has no code set.
        ``naics_version`` returns ``None`` here because its coverage is per-code.
    """
    if column == "lfo":
        return LFO, ""
    if column in TOTAL_NOISE_COLUMNS:
        return NOISE, "2017(1)2023"
    if column.endswith("_noise_flag"):
        return BAND_NOISE, "2017(1)2023"
    if column == "employment_flag":
        return EMPFLAG, "1998(1)2016"
    if column.endswith("_flag"):
        return BAND_EMPFLAG, "1998(1)2016"
    return None


def covered_columns(table: str) -> list[str]:
    """List the columns a table's architecture sheet marks as dictionary-covered.

    Args:
        table: Table slug (``national``, ``state`` or ``county``).

    Returns:
        Column names whose ``covered_by_dictionary`` cell is ``yes``, in sheet
        order.
    """
    with open(HERE / f"sheet_{table}.tsv", newline="") as f:
        return [
            r["name"]
            for r in csv.DictReader(f, delimiter="\t")
            if r["covered_by_dictionary"] == "yes"
        ]


def main() -> None:
    """Emit one dictionary row per (table, coded column, code) to parquet."""
    rows = []
    for table in TABLES:
        for col in covered_columns(table):
            if col == "naics_version":
                pairs = [
                    (k, lab, cov) for k, (lab, cov) in NAICS_VERSION.items()
                ]
            else:
                mapped = code_map(col)
                if mapped is None:
                    raise ValueError(
                        f"{table}.{col} is covered_by_dictionary=yes but has no "
                        f"code set in gen_dicionario.py"
                    )
                codes, cov = mapped
                pairs = [(k, lab, cov) for k, lab in codes.items()]
            for key, label, cov in pairs:
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": col,
                        "chave": key,
                        "cobertura_temporal": cov,
                        "valor": label,
                    }
                )
    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    dest = OUTPUT / "dicionario"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        dest / "data.parquet",
        compression="snappy",
    )
    print(f"dicionario: {len(df)} rows -> {dest / 'data.parquet'}")


if __name__ == "__main__":
    main()
