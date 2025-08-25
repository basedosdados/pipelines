# -*- coding: utf-8 -*-
import os
from pathlib import Path

import basedosdados as bd
import pandas as pd
from utils import (
    convert_to_pd_dtype,
    drop_empty_lines,
    get_disciplina_serie,
    get_nivel_serie_disciplina,
)

CWD = Path(os.getcwd()).parent

INPUT = CWD / "input"
OUTPUT = CWD / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

ufs_saeb_latest = pd.read_excel(
    INPUT / "PLANILHAS DE RESULTADOS_20250507" / "TS_UF_20250507.xlsx",
    dtype=str,
)

ufs_saeb_latest.head()

ufs_saeb_latest = (
    ufs_saeb_latest.drop(0, axis="index")
    .pipe(lambda df: df.loc[df["CAPITAL"] == "Total"])
    .drop(columns=["CAPITAL", "CO_UF"])
)

ufs_saeb_latest.columns.tolist()

ufs_saeb_nivel_long_fmt = pd.melt(
    ufs_saeb_latest,
    id_vars=[
        "NO_UF",
        "DEPENDENCIA_ADM",
        "LOCALIZACAO",
    ],
    value_vars=[
        col
        for col in ufs_saeb_latest.columns.tolist()
        if col.startswith("nivel")
    ],
)

ufs_saeb_media_long_fmt = pd.melt(
    ufs_saeb_latest,
    id_vars=[
        "NO_UF",
        "DEPENDENCIA_ADM",
        "LOCALIZACAO",
    ],
    value_vars=[
        col
        for col in ufs_saeb_latest.columns.tolist()
        if col.startswith("MEDIA")
    ],
)

ufs_saeb_media_long_fmt = (
    ufs_saeb_media_long_fmt.assign(
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


ufs_saeb_nivel_long_fmt = (
    ufs_saeb_nivel_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(
            get_nivel_serie_disciplina
        )
    )
    .assign(
        nivel=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
        serie=lambda df: df["parsed_variable"]
        .apply(lambda v: v[2])
        .replace({"EMT": 12, "EMI": 13, "EM": 14})
        .astype("string")
        .astype("Int64"),
    )
    .drop(columns=["parsed_variable"])
)

ufs_saeb_latest_output = (
    (
        ufs_saeb_nivel_long_fmt.pivot(
            index=[
                "NO_UF",
                "DEPENDENCIA_ADM",
                "LOCALIZACAO",
                "disciplina",
                "serie",
            ],
            columns="nivel",
            values="value",
        )
        .reset_index()
        .merge(
            ufs_saeb_media_long_fmt.rename(columns={"value": "media"}),
            left_on=[
                "NO_UF",
                "DEPENDENCIA_ADM",
                "LOCALIZACAO",
                "disciplina",
                "serie",
            ],
            right_on=[
                "NO_UF",
                "DEPENDENCIA_ADM",
                "LOCALIZACAO",
                "disciplina",
                "serie",
            ],
        )
    )
    .drop(columns=["variable"])
    .rename(columns={i: f"nivel_{i}" for i in range(0, 11)})
)


## Clean step

ufs_saeb_latest_output.head()

ufs_saeb_latest_output["serie"].unique()
ufs_saeb_latest_output["disciplina"].unique()

bd_dirs_ufs = bd.read_sql(
    "select sigla, nome from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

ufs_saeb_latest_output = (
    # Apenas MT e LP. Não sei porque não subiram outras disciplinas
    ufs_saeb_latest_output.loc[
        ufs_saeb_latest_output["disciplina"].isin(["MT", "LP"])
    ]
    .assign(
        rede=lambda df: df["DEPENDENCIA_ADM"].str.lower(),
        localizacao=lambda df: df["LOCALIZACAO"].str.lower(),
        sigla_uf=lambda df: df["NO_UF"].replace(
            dict(
                [
                    (i["nome"], i["sigla"])
                    for i in bd_dirs_ufs.to_dict("records")
                ]
            )  # type: ignore
        ),
    )
    .drop(columns=["NO_UF", "DEPENDENCIA_ADM", "LOCALIZACAO"])
)

# Add column ano
ufs_saeb_latest_output["ano"] = 2023

ufs_saeb_latest_output.head()

ufs_saeb_latest_output.info()

ufs_saeb_latest_output.shape

ufs_saeb_latest_output = drop_empty_lines(ufs_saeb_latest_output)

tb = bd.Table(dataset_id="br_inep_saeb", table_id="uf")

bq_cols = tb._get_columns_from_bq(mode="prod")

assert len(bq_cols["partition_columns"]) == 0

col_dtypes = {
    col["name"]: convert_to_pd_dtype(col["type"]) for col in bq_cols["columns"]
}

# Order columns
ufs_saeb_latest_output = ufs_saeb_latest_output.astype(col_dtypes)[
    col_dtypes.keys()
]

upstream_df = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_saeb.uf`",
    billing_project_id="basedosdados-dev",
)

upstream_df["serie"].unique()

pd.concat([ufs_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "uf.csv"), index=False
)

# Update table
tb.create(
    os.path.join(OUTPUT, "uf.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
