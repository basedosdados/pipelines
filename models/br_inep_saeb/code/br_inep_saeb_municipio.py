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

mun_saeb_latest = pd.read_excel(
    INPUT / "PLANILHAS DE RESULTADOS_20250507" / "TS_MUNICIPIO_20250507.xlsx",
    dtype=str,
)

mun_saeb_latest.head()

mun_saeb_latest = (
    mun_saeb_latest.drop(0, axis="index")
    .drop(columns=["CO_UF", "NO_MUNICIPIO"])
    .rename(
        columns={
            "NO_UF": "nome_uf",
            "DEPENDENCIA_ADM": "rede",
            "LOCALIZACAO": "localizacao",
            "CO_MUNICIPIO": "id_municipio",
        }
    )
)

mun_saeb_latest.columns.tolist()

mun_saeb_nivel_long_fmt = pd.melt(
    mun_saeb_latest,
    id_vars=[
        "nome_uf",
        "id_municipio",
        "rede",
        "localizacao",
    ],
    value_vars=[
        col
        for col in mun_saeb_latest.columns.tolist()
        if col.startswith("nivel")
    ],
)

mun_saeb_media_long_fmt = pd.melt(
    mun_saeb_latest,
    id_vars=[
        "nome_uf",
        "id_municipio",
        "rede",
        "localizacao",
    ],
    value_vars=[
        col
        for col in mun_saeb_latest.columns.tolist()
        if col.startswith("MEDIA")
    ],
)

mun_saeb_media_long_fmt = (
    mun_saeb_media_long_fmt.assign(
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


mun_saeb_nivel_long_fmt = (
    mun_saeb_nivel_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(
            get_nivel_serie_disciplina
        )
    )
    .assign(
        nivel=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
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

mun_saeb_latest_output = (
    (
        mun_saeb_nivel_long_fmt.pivot_table(
            index=[
                "nome_uf",
                "id_municipio",
                "rede",
                "localizacao",
                "disciplina",
                "serie",
            ],
            columns="nivel",
            values="value",
        )
        .reset_index()
        .merge(
            mun_saeb_media_long_fmt.rename(columns={"value": "media"}),
            left_on=[
                "nome_uf",
                "id_municipio",
                "rede",
                "localizacao",
                "disciplina",
                "serie",
            ],
            right_on=[
                "nome_uf",
                "id_municipio",
                "rede",
                "localizacao",
                "disciplina",
                "serie",
            ],
            how="left",
        )
    )
    .drop(columns=["variable"])
    .rename(columns={i: f"nivel_{i}" for i in range(0, 11)})
)


## Clean step

mun_saeb_latest_output.head()

mun_saeb_latest_output["serie"].unique()
mun_saeb_latest_output["disciplina"].unique()
mun_saeb_latest_output["localizacao"].unique()
mun_saeb_latest_output["rede"].unique()

bd_dirs_ufs = bd.read_sql(
    "select sigla, nome from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)


mun_saeb_latest_output = (
    # Apenas MT e LP
    mun_saeb_latest_output.loc[
        mun_saeb_latest_output["disciplina"].isin(["MT", "LP"])
    ]
    .assign(
        disciplina=lambda df: df["disciplina"].str.upper(),
        rede=lambda df: df["rede"].str.lower(),
        localizacao=lambda df: df["localizacao"].str.lower(),
        sigla_uf=lambda df: df["nome_uf"].replace(
            dict(
                [
                    (i["nome"], i["sigla"])
                    for i in bd_dirs_ufs.to_dict("records")
                ]
            )  # type: ignore
        ),
    )
    .drop(columns=["nome_uf"])
)

mun_saeb_latest_output = drop_empty_lines(mun_saeb_latest_output)

mun_saeb_latest_output["ano"] = 2023

mun_saeb_latest_output.head()

mun_saeb_latest_output.info()

tb = bd.Table(dataset_id="br_inep_saeb", table_id="municipio")

bq_cols = tb._get_columns_from_bq(mode="prod")

assert len(bq_cols["partition_columns"]) == 0

col_dtypes = {
    col["name"]: convert_to_pd_dtype(col["type"]) for col in bq_cols["columns"]
}

# Order columns
mun_saeb_latest_output = mun_saeb_latest_output.astype(col_dtypes)[
    col_dtypes.keys()
]

upstream_df = bd.read_sql(
    "select * from `basedosdados.br_inep_saeb.municipio`",
    billing_project_id="basedosdados-dev",
)

pd.concat([mun_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "municipio.csv"), index=False
)

# Update table
tb.create(
    os.path.join(OUTPUT, "municipio.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
