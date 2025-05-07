# -*- coding: utf-8 -*-
"""
Script para baixar is dados da taxa de alfabetização divulgado pelo SAEB.
Fonte: https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/saeb/resultados

São duas tabelas:
- brasil_taxa_alfabetizacao
- uf_taxa_alfabetizacao
"""

import os

import basedosdados as bd
import pandas as pd

os.getcwd()

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

URL = "https://download.inep.gov.br/saeb/resultados/saeb_2021_brasil_estados_municipios_c_tx_alfabetizado.xlsx"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

os.system(f"cd {INPUT}; curl -O -k {URL}")

# Brasil

df_br = pd.read_excel(
    os.path.join(INPUT, os.path.basename(URL)), sheet_name="Brasil"
)

df_br.columns

df_br["ID"].unique()

df_br["ANO_SAEB"].unique()

df_br["DEPENDENCIA_ADM"].unique()

df_br["LOCALIZACAO"].unique()

df_br["CAPITAL"].unique()

df_br["TX_ALFABETIZADO"].unique()

df_br["DEPENDENCIA_ADM"] = df_br["DEPENDENCIA_ADM"].str.lower()

df_br["LOCALIZACAO"] = df_br["LOCALIZACAO"].str.lower()

df_br["CAPITAL"] = df_br["CAPITAL"].str.lower()

df_br = df_br.drop(columns=["ID", "MEDIA_2_LP", "MEDIA_2_MT"])

df_br = df_br.rename(
    columns={
        "ANO_SAEB": "ano",
        "DEPENDENCIA_ADM": "rede",
        "LOCALIZACAO": "localizacao",
        "CAPITAL": "area",
        "TX_ALFABETIZADO": "taxa_alfabetizacao",
    },
    errors="raise",
)

df_br_from_bigquery = bd.read_sql(
    "select * from `basedosdados.br_inep_saeb.brasil_taxa_alfabetizacao`",
    billing_project_id="basedosdados-dev",
)

pd.concat([df_br_from_bigquery, df_br]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "brasil_taxa_alfabetizacao.csv"), index=False
)

# Estados

df_ufs = pd.read_excel(
    os.path.join(INPUT, os.path.basename(URL)), sheet_name="Estados"
)

df_ufs["DEPENDENCIA_ADM"] = df_ufs["DEPENDENCIA_ADM"].str.lower()

df_ufs["LOCALIZACAO"] = df_ufs["LOCALIZACAO"].str.lower()

df_ufs["CAPITAL"] = df_ufs["CAPITAL"].str.lower()

bd_dirs_ufs = bd.read_sql(
    "select sigla, nome from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

uf_map = dict(
    [(i["nome"], i["sigla"]) for i in bd_dirs_ufs.to_dict("records")]
)  # type: ignore

df_ufs["NO_UF"].unique()

df_ufs["NO_UF"] = df_ufs["NO_UF"].replace(uf_map)

df_ufs["NO_UF"].isna().sum()

df_ufs = df_ufs.drop(columns=["CO_UF", "MEDIA_2_LP", "MEDIA_2_MT"])

df_ufs = df_ufs.rename(
    columns={
        "ANO_SAEB": "ano",
        "NO_UF": "sigla_uf",
        "DEPENDENCIA_ADM": "rede",
        "LOCALIZACAO": "localizacao",
        "CAPITAL": "area",
        "TX_ALFABETIZADO": "taxa_alfabetizacao",
    },
    errors="raise",
)

df_ufs_from_bigquery = bd.read_sql(
    "select * from `basedosdados.br_inep_saeb.uf_taxa_alfabetizacao`",
    billing_project_id="basedosdados-dev",
)

pd.concat([df_ufs_from_bigquery, df_ufs]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "uf_taxa_alfabetizacao.csv"), index=False
)

# Upload

## Brasil

tb_br = bd.Table(
    dataset_id="br_inep_saeb", table_id="brasil_taxa_alfabetizacao"
)
tb_br.create(os.path.join(OUTPUT, "brasil_taxa_alfabetizacao.csv"))


tb_uf = bd.Table(dataset_id="br_inep_saeb", table_id="uf_taxa_alfabetizacao")
tb_uf.create(os.path.join(OUTPUT, "uf_taxa_alfabetizacao.csv"))
