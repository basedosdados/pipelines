"""Build the us_census_lodes `dicionario` table.

The only codified column in this dataset is `job_type`, whose six values come
from the `[TYPE]` component of the LODES file name. Every other coded-looking
column (block, tract, county and state identifiers) resolves through a
directory, not through this dictionary.

    uv run python models/us_census_lodes/code/gen_dicionario.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import (
    JOB_TYPE_LABELS,
    OUTPUT,
    YEARS,
)
from pipelines.datasets.us_census_lodes.utils import (
    write_parquet,
)

# JT04 and JT05 (federal jobs) are supplied by OPM and only exist from 2010.
FIRST_YEAR = {"JT04": 2010, "JT05": 2010}


def build() -> pd.DataFrame:
    rows = []
    for table in ("residence_jobs", "workplace_jobs"):
        for code, (en, _pt, _es) in JOB_TYPE_LABELS.items():
            start = FIRST_YEAR.get(code, YEARS[0])
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": "job_type",
                    "chave": code,
                    "cobertura_temporal": f"{start}(1){YEARS[-1]}",
                    "valor": en,
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    df = build()
    rows = write_parquet(
        df, "dicionario", OUTPUT / "dicionario" / "data.parquet"
    )
    print(
        f"dicionario: {rows} rows -> {OUTPUT / 'dicionario' / 'data.parquet'}"
    )
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
