# -*- coding: utf-8 -*-
import os
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from utils import (
    convert_to_pd_dtype,
    get_disciplina_serie,
    get_nivel_serie_disciplina,
)

CWD = Path(os.getcwd()).parent

INPUT = CWD / "input"
OUTPUT = CWD / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/microdados/planilhas_de_resultados_20250507.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "2023.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2023.zip") as z:
    z.extractall(INPUT)

br_saeb_latest = pd.read_excel(
    INPUT / "PLANILHAS DE RESULTADOS_20250507" / "TS_BRASIL_20250507.xlsx",
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
        serie=lambda df: df["parsed_variable"]
        .apply(lambda v: v[1])
        .astype("Int64"),
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
        serie=lambda df: df["parsed_variable"]
        .apply(lambda v: v[2])
        .replace({"EMT": 12, "EMI": 13, "EM": 14})
        .astype("string")
        .astype("Int64"),
    )
    .drop(columns=["parsed_variable"])
)

br_saeb_latest_output = (
    (
        br_saeb_nivel_long_fmt.pivot(
            index=["DEPENDENCIA_ADM", "LOCALIZACAO", "disciplina", "serie"],
            columns="nivel",
            values="value",
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

br_saeb_latest_output["ano"] = 2023

br_saeb_latest_output.head()

br_saeb_latest_output.info()

br_saeb_latest_output.shape

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

upstream_df.shape

br_saeb_latest_output.shape

pd.concat([br_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "brasil.csv"), index=False
)

# Update table
tb.create(
    os.path.join(OUTPUT, "brasil.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
