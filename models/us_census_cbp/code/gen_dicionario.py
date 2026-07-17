"""Build the us_census_cbp dicionario table (coded value -> label) as parquet.

Covers the total-level coded columns; the per-size-band flag columns share these
same code sets (documented in their column descriptions). Codes from CBP record
layouts (LFO, EMPFLAG, noise flags) and the NAICS-vintage mapping.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT = Path(__file__).resolve().parents[1] / "data" / "output"

# CBP uses NAICS 2017 through 2023 (no NAICS 2022 vintage in this data).
NAICS_VERSION = {
    "1997": "NAICS 1997",
    "2002": "NAICS 2002",
    "2007": "NAICS 2007",
    "2012": "NAICS 2012",
    "2017": "NAICS 2017",
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
    "G": "Low noise, 0 to less than 2 percent",
    "H": "Medium noise, 2 to less than 5 percent",
    "J": "High noise, 5 percent or more",
    "N": "Not available or not comparable",
}

# (column, code-map, temporal_coverage, tables it appears in)
SPEC = [
    (
        "naics_version",
        NAICS_VERSION,
        "1998(1)2023",
        ["national", "state", "county"],
    ),
    ("lfo", LFO, "", ["national", "state"]),
    (
        "employment_flag",
        EMPFLAG,
        "1998(1)2016",
        ["national", "state", "county"],
    ),
    (
        "employment_noise_flag",
        NOISE,
        "2017(1)2023",
        ["national", "state", "county"],
    ),
    (
        "payroll_first_quarter_noise_flag",
        NOISE,
        "2017(1)2023",
        ["national", "state", "county"],
    ),
    (
        "payroll_annual_noise_flag",
        NOISE,
        "2017(1)2023",
        ["national", "state", "county"],
    ),
]


def main():
    rows = []
    for col, codes, cov, tables in SPEC:
        for table in tables:
            for key, label in codes.items():
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
