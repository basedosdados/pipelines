import os

import basedosdados as bd
import pandas as pd
from utils import (
    RENAMES_MUN,
    convert_to_pd_dtype,
    drop_empty_lines,
    get_disciplina_serie,
    get_nivel_serie_disciplina,
)

CWD = os.path.dirname(os.getcwd())

INPUT = os.path.join(CWD, "input")
OUTPUT = os.path.join(CWD, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

mun_saeb_latest = pd.read_excel(
    os.path.join(INPUT, "saeb_2021_brasil_estados_municipios.xlsx"),
    dtype=str,
    sheet_name="Municípios",
)

mun_saeb_latest.head()

mun_saeb_latest = (
    mun_saeb_latest.drop(0, axis="index")
    .rename(
        # Algumas colunas que tem em estados nao tem em municipio
        columns={
            k: v
            for k, v in RENAMES_MUN.items()
            if k in mun_saeb_latest.columns
        },
        errors="raise",
    )
    .drop(columns=["CO_UF", "NO_MUNICIPIO"])
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
        if col.startswith("media")
    ],
)

mun_saeb_media_long_fmt = (
    mun_saeb_media_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_disciplina_serie)
    )
    .assign(
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
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
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[2]),
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
        mun_saeb_latest_output["disciplina"].isin(["mt", "lp"])
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
        serie=lambda df: df["serie"].replace(
            {
                # em é 12
                "em": "12",
                # em_integral (Ensino Medio Integrado) é 13
                "em_integral": "13",
                # em_regular (Ensino Médio Tradicional + Integrado) é 14
                "em_regular": "14",
            }
        ),
    )
    .drop(columns=["nome_uf"])
)

mun_saeb_latest_output = drop_empty_lines(mun_saeb_latest_output)

mun_saeb_latest_output["ano"] = 2021

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
    "select * from `basedosdados.br_inep_saeb.municipio` where ano <> 2021",
    billing_project_id="basedosdados-dev",
)

assert isinstance(upstream_df, pd.DataFrame)

# upstream_df["serie"].unique()
#
# upstream_df["serie"] = upstream_df["serie"].replace({3: 12})

upstream_df = drop_empty_lines(upstream_df)

pd.concat([mun_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "municipio.csv"), index=False
)

# Update table
tb.create(
    os.path.join(OUTPUT, "municipio.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
