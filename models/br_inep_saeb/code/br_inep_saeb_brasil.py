import os
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

from models.br_inep_saeb.code.utils import (
    convert_to_pd_dtype,
    get_disciplina_serie,
    get_nivel_serie_disciplina,
)

input = Path("input") / "br_inep_saeb"
output = Path("output") / "br_inep_saeb"

os.makedirs(input, exist_ok=True)
os.makedirs(output, exist_ok=True)

url = "https://download.inep.gov.br/saeb/resultados/saeb_2025_brasil_estados_municipios_censitario.xlsx"
xlsx_file = "saeb_2025.xlsx"


def download(url: str, max_attempts: int = 3, timeout: float = 120):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                verify=False,
                stream=True,
                timeout=timeout,
            )
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError as exc:
            print(
                f"[attempt {attempt}/{max_attempts}] failed: {exc!r} - retrying.."
            )
    raise Exception(f"All {max_attempts} attempts failed for {url}")


r = download(url)

with open(input / xlsx_file, "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

br_saeb_latest = pd.read_excel(
    input / xlsx_file,
    sheet_name="Brasil",
    dtype=str,
)

br_saeb_latest.head()

br_saeb_latest = (
    br_saeb_latest.drop(0, axis="index")
    .pipe(lambda df: df.loc[df["CAPITAL"] == "Total"])
    .drop(columns=["CAPITAL", "ID"])
)

br_saeb_latest.head()

br_saeb_nivel_long_fmt = pd.melt(
    br_saeb_latest,
    id_vars=[
        "DEPENDENCIA_ADM",
        "LOCALIZACAO",
    ],
    value_vars=[
        col
        for col in br_saeb_latest.columns.tolist()
        if col.startswith("nivel")
    ],
)

br_saeb_media_long_fmt = pd.melt(
    br_saeb_latest,
    id_vars=[
        "DEPENDENCIA_ADM",
        "LOCALIZACAO",
    ],
    value_vars=[
        col
        for col in br_saeb_latest.columns.tolist()
        if col.startswith("MEDIA")
    ],
)


br_saeb_media_long_fmt = (
    br_saeb_media_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_disciplina_serie)
    )
    .assign(
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        serie=lambda df: (
            df["parsed_variable"].apply(lambda v: v[1]).astype("Int64")
        ),
    )
    .drop(columns=["parsed_variable"])
)


br_saeb_nivel_long_fmt = (
    br_saeb_nivel_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(
            get_nivel_serie_disciplina
        )
    )
    .assign(
        nivel=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
        # EMT = Ensino Médio Tradicional
        # EMI = Ensino Médio Integrado
        # EM = Ensino Médio (Tradicional + Integrado)
        serie=lambda df: (
            df["parsed_variable"]
            .apply(lambda v: v[2])
            .replace({"EMT": 12, "EMI": 13, "EM": 14})
            .astype("string")
            .astype("Int64")
        ),
    )
    .drop(columns=["parsed_variable"])
)

br_saeb_latest_output = (
    (
        br_saeb_nivel_long_fmt.pivot_table(
            index=["DEPENDENCIA_ADM", "LOCALIZACAO", "disciplina", "serie"],
            columns="nivel",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .merge(
            br_saeb_media_long_fmt.rename(columns={"value": "media"}),
            left_on=["DEPENDENCIA_ADM", "LOCALIZACAO", "disciplina", "serie"],
            right_on=["DEPENDENCIA_ADM", "LOCALIZACAO", "disciplina", "serie"],
        )
    )
    .drop(columns=["variable"])
    .rename(columns={i: f"nivel_{i}" for i in range(0, 11)})
    .rename(columns={"DEPENDENCIA_ADM": "rede", "LOCALIZACAO": "localizacao"})
)

## Clean step

br_saeb_latest_output.head()

br_saeb_latest_output["serie"].unique()
br_saeb_latest_output["disciplina"].unique()
br_saeb_latest_output["localizacao"].unique()
br_saeb_latest_output["rede"].unique()

br_saeb_latest_output = (
    # apenas MT e LP
    br_saeb_latest_output.loc[
        br_saeb_latest_output["disciplina"].isin(["MT", "LP"])
    ].assign(
        disciplina=lambda df: df["disciplina"].str.upper(),
        rede=lambda df: df["rede"].str.lower(),
        localizacao=lambda df: df["localizacao"].str.lower(),
    )
)

br_saeb_latest_output["ano"] = 2025

br_saeb_latest_output.head()

br_saeb_latest_output.info()

tb = bd.Table(dataset_id="br_inep_saeb", table_id="brasil")

bq_cols = tb._get_columns_from_bq(mode="prod")

assert len(bq_cols["partition_columns"]) == 0

col_dtypes = {
    col["name"]: convert_to_pd_dtype(col["type"]) for col in bq_cols["columns"]
}

# Order columns
br_saeb_latest_output = br_saeb_latest_output.astype(col_dtypes)[
    col_dtypes.keys()
]

upstream_df = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_saeb.brasil`",
    billing_project_id="basedosdados-dev",
)

br_saeb_updated = pd.concat([br_saeb_latest_output, upstream_df])

br_saeb_updated.to_csv(os.path.join(output, "brasil.csv"), index=False)

print(br_saeb_updated)

# Update table
tb.create(
    output / "brasil.csv",
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
