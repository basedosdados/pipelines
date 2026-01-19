"""
Script para atualizar a tabela do dataset br_inep_indicador_nivel_socioeconomico
"""

from pathlib import Path

import basedosdados as bd
import pandas as pd

input = Path("input")
output = Path("output") / "br_inep_indicador_nivel_socioeconomico"

inse_escola_2021 = pd.read_excel(
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2021/nivel_socioeconomico/INSE_2021_escolas.xlsx"
)
inse_escola_2023 = pd.read_excel(
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/nivel_socioeconomico/INSE_2023_escolas.xlsx"
)

df_inse_bd = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_indicador_nivel_socioeconomico_staging.escola`",
    billing_project_id="basedosdados-dev",
)

renames_inse_escola = {
    "NU_ANO_SAEB": "ano",
    "SG_UF": "sigla_uf",
    "CO_MUNICIPIO": "id_municipio",
    "ID_ESCOLA": "id_escola",
    "TP_TIPO_REDE": "rede",
    "TP_LOCALIZACAO": "tipo_localizacao",
    "TP_CAPITAL": "area",
    "QTD_ALUNOS_INSE": "quantidade_alunos_inse",
    "MEDIA_INSE": "inse",
    "INSE_CLASSIFICACAO": "classificacao",
    "PC_NIVEL_1": "percentual_nivel_1",
    "PC_NIVEL_2": "percentual_nivel_2",
    "PC_NIVEL_3": "percentual_nivel_3",
    "PC_NIVEL_4": "percentual_nivel_4",
    "PC_NIVEL_5": "percentual_nivel_5",
    "PC_NIVEL_6": "percentual_nivel_6",
    "PC_NIVEL_7": "percentual_nivel_7",
    "PC_NIVEL_8": "percentual_nivel_8",
}

inse_2021_2023 = pd.concat(
    [
        inse_escola_2021.rename(columns=renames_inse_escola, errors="raise"),
        inse_escola_2023.rename(columns=renames_inse_escola, errors="raise"),
    ]
)[renames_inse_escola.values()]


df_inse_escola_updated = pd.concat(
    [df_inse_bd.loc[df_inse_bd.ano != "2021"], inse_2021_2023]
)

output_inse_escola = output / "escola"

output_inse_escola.mkdir(exist_ok=True, parents=True)

df_inse_escola_updated.to_csv(output_inse_escola / "data.csv", index=False)

tb_escola = bd.Table("br_inep_indicador_nivel_socioeconomico", "escola")

tb_escola.create(
    output_inse_escola / "data.csv",
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
