"""
Script para atualizar a tabela br_inep_indicador_nivel_socioeconomico.dicionario para 2023
"""

from pathlib import Path

import basedosdados as bd
import pandas as pd

input = Path("input")
output = Path("output") / "br_inep_indicador_nivel_socioeconomico"

inse_escola_2023 = pd.read_excel(
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/nivel_socioeconomico/INSE_2023_escolas.xlsx",
    sheet_name="Dicionário",
)

dicionario = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_indicador_nivel_socioeconomico_staging.dicionario`",
    billing_project_id="basedosdados-dev",
)


def add_new_year(coluna: str, chave: str, cobertura_temporal: str) -> str:
    if (coluna == "rede" and chave == "4") or (
        coluna == "classificacao" and not chave.startswith("Nível")
    ):
        return cobertura_temporal
    else:
        return f"{cobertura_temporal}, 2023"


dicionario_escola = dicionario.loc[dicionario.id_tabela == "escola"]

dicionario_escola["cobertura_temporal"] = dicionario_escola[
    ["coluna", "chave", "cobertura_temporal"]
].apply(lambda v: add_new_year(v[0], v[1], v[2]), axis=1)

pd.concat(
    [dicionario_escola, dicionario.loc[dicionario.id_tabela != "escola"]]
).to_csv(output / "data.csv", index=False)

tb = bd.Table("br_inep_indicador_nivel_socioeconomico", "dicionario")

tb.create(
    output / "data.csv",
    if_storage_data_exists="replace",
    if_table_exists="replace",
)
