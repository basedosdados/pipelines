import os

import pandas as pd

# Antigo arquivo de pais sem COI e FIFA
original = pd.read_csv(
    "https://storage.googleapis.com/basedosdados/staging/br_bd_diretorios_mundo/pais/pais.csv"
)

html = "https://pt.wikipedia.org/wiki/Compara%C3%A7%C3%A3o_entre_c%C3%B3digos_de_pa%C3%ADses_COI,_FIFA,_e_ISO_3166"

attrs = {"class": "wikitable"}

dfs = pd.read_html(html, attrs=attrs, header=0)

df_dif = pd.concat([df for df in dfs])

ico_fifa = df_dif[["COI", "FIFA", "ISO-3"]]
ico_fifa.columns = ["sigla_pais_coi", "sigla_pais_fifa", "sigla_pais_iso3"]

merge_df = original.merge(
    ico_fifa, left_on="sigla_pais_iso3", right_on="sigla_pais_iso3", how="left"
)

os.makedirs("output", exist_ok=True)

merge_df.to_csv("output/pais.csv", index=False)
