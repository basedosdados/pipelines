import os
import zipfile
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
import requests

CWD = Path("models") / "br_inep_saeb"
INPUT = CWD / "input"
OUTPUT = CWD / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/microdados/microdados_saeb_2023.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "microdados_saeb_2023.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "microdados_saeb_2023.zip") as z:
    z.extractall(INPUT)

bd_dir_uf = bd.read_sql(
    "select id_uf, sigla from `basedosdados-dev.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)


def possui_necessidade_especial(a: str, b: str, c: str) -> str:
    empty = ["*", "."]
    values = [a, b, c]

    for value in values:
        if value not in empty:
            return value

    return a


def transporte_escolar(a: str, b: str) -> str:
    empty = ["*", "."]
    values = [a, b]

    for value in values:
        if value not in empty:
            return value

    return a


def mora_avos(a: str, b: str) -> str:
    empty = ["*", "."]
    values = [a, b]

    for value in values:
        if value not in empty:
            return value

    return a


def batch_process(
    df: pd.DataFrame,
    id_vars: list[str],
    value_vars: list[str],
    batch_size: int = 100_000,
) -> list[pd.DataFrame]:
    result = []

    for _, batch_df in enumerate(np.array_split(df, len(df) // batch_size)):  # type: ignore
        assert isinstance(batch_df, pd.DataFrame)

        batch_df_long = batch_df.melt(
            id_vars=id_vars, value_vars=value_vars
        ).assign(
            disciplina=lambda d: d["variable"].apply(
                lambda v: "LP" if "LP" in v else "MT"
            ),
            variable=lambda d: d["variable"].apply(
                lambda v: v.replace("LP", "").replace("MT", "")
            ),
        )

        batch_df_wide = batch_df_long.pivot(
            index=[
                col
                for col in batch_df_long.columns
                if col not in ["variable", "value"]
            ],
            columns="variable",
            values="value",
            # aggfunc="first",
        ).reset_index()

        result.append(batch_df_wide)

    return result


# 2 ano

df_aluno_ef_2ano = pd.read_csv(
    INPUT / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_2EF.csv", sep=";"
)

value_vars_2ano = [
    i for i in df_aluno_ef_2ano.columns if "MT" in i or "LP" in i
]

id_vars_2ano = list(set(df_aluno_ef_2ano.columns) - set(value_vars_2ano))

df_long_2ano = df_aluno_ef_2ano.melt(
    id_vars=id_vars_2ano, value_vars=value_vars_2ano
).assign(
    disciplina=lambda d: d["variable"].apply(
        lambda v: "LP" if "LP" in v else "MT"
    ),
    variable=lambda d: d["variable"].apply(
        lambda v: v.replace("LP", "").replace("MT", "")
    ),
)

df_wide_2ano = df_long_2ano.pivot_table(
    index=[
        col for col in df_long_2ano.columns if col not in ["variable", "value"]
    ],
    columns="variable",
    values="value",
    aggfunc="first",
).reset_index()


renames_2ano = {
    "ID_SAEB": "ano",
    "ID_ESCOLA": "id_escola",
    "CO_CONCEITO_SEGMENTACAO": "conceito_segmentacao",
    "CO_RESPOSTA_TEXTO": "resposta_texto",
    "ID_REGIAO": "id_regiao",
    "ID_UF": "id_uf",
    "ID_SERIE": "serie",
    "IN_SITUACAO_CENSO": "situacao_censo",
    "ID_MUNICIPIO": "id_municipio",
    "ID_TURMA": "id_turma",
    "ID_AREA": "area",
    "ESTRATO": "estrato",
    "IN_AMOSTRA": "amostra",
    "ID_LOCALIZACAO": "localizacao",
    "ID_ALUNO": "id_aluno",
    "ID_CADERNO_": "caderno",
    "CO_TEXTO_GRAFIA": "texto_grafia",
    "PROFICIENCIA__SAEB": "proficiencia_saeb",
    "ERRO_PADRAO_": "erro_padrao",
    "ERRO_PADRAO__SAEB": "erro_padrao_saeb",
    "PESO_ALUNO_": "peso_aluno",
    "ID_BLOCO_1_": "bloco_1",
    "ID_BLOCO_2_": "bloco_2",
    "CO_CONCEITO_Q1_": "conceito_q1",
    "CO_CONCEITO_Q2_": "conceito_q2",
    "IN_PREENCHIMENTO_": "preenchimento_caderno",
    "IN_PRESENCA_": "presenca",
    "IN_PROFICIENCIA_": "indicador_proficiencia",
    "NU_BLOCO_1_ABERTA_": "bloco_1_aberto",
    "NU_BLOCO_2_ABERTA_": "bloco_2_aberto",
    "PROFICIENCIA_": "proficiencia",
    "TX_RESP_BLOCO1_": "respostas_bloco_1",
    "TX_RESP_BLOCO2_": "respostas_bloco_2",
}


upstream_2ano_columns = bd.read_sql(
    "select * from basedosdados-dev.br_inep_saeb_staging.aluno_ef_2ano limit 0",
    billing_project_id="basedosdados-dev",
).columns.tolist()


df_wide_2ano = (
    df_wide_2ano.rename(columns=renames_2ano, errors="raise")
    .assign(
        sigla_uf=lambda d: d["id_uf"]
        .astype("string")
        .replace(
            {i["id_uf"]: i["sigla"] for i in bd_dir_uf.to_dict("records")}
        )
    )
    .drop(columns=["id_uf"])
)

missing_cols_2ano = [
    i for i in upstream_2ano_columns if i not in df_wide_2ano.columns
]

df_wide_2ano[missing_cols_2ano] = None

df_wide_2ano["rede"].unique()

df_wide_2ano["rede"] = df_wide_2ano["IN_PUBLICA"]

df_wide_2ano = df_wide_2ano[upstream_2ano_columns]

for (ano, sigla_uf), df_ano_uf_disc in df_wide_2ano.groupby(  # type: ignore
    ["ano", "sigla_uf"]
):
    path = OUTPUT / "aluno_ef_2ano" / f"ano={ano}" / f"sigla_uf={sigla_uf}"
    path.mkdir(parents=True, exist_ok=True)
    df_ano_uf_disc.drop(columns=["ano", "sigla_uf"]).to_csv(
        path / "data.csv", index=False
    )

tb_2ano = bd.Table(dataset_id="br_inep_saeb", table_id="aluno_ef_2ano")

tb_2ano.create(
    path=OUTPUT / "aluno_ef_2ano",
    if_table_exists="replace",
    if_storage_data_exists="replace",
)

# 5 ano

df_aluno_ef_5ano = pd.read_csv(
    INPUT / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_5EF.csv", sep=";"
)

value_vars_5ano = [
    i for i in df_aluno_ef_5ano.columns if "MT" in i or "LP" in i
]

id_vars_5ano = list(set(df_aluno_ef_5ano.columns) - set(value_vars_5ano))


df_wide_5ano = pd.concat(
    batch_process(
        df_aluno_ef_5ano, value_vars=value_vars_5ano, id_vars=id_vars_5ano
    )
)

df_wide_5ano.columns.to_list()


renames_5ano = {
    "ID_SAEB": "ano",
    "ID_REGIAO": "id_regiao",
    "ID_UF": "id_uf",
    "ID_MUNICIPIO": "id_municipio",
    "ID_AREA": "id_area",
    "ID_ESCOLA": "id_escola",
    "IN_PUBLICA": "escola_publica",
    "ID_LOCALIZACAO": "localizacao",
    "ID_TURMA": "id_turma",
    "ID_SERIE": "serie",
    "ID_ALUNO": "id_aluno",
    "IN_SITUACAO_CENSO": "situacao_censo",
    "IN_PREENCHIMENTO_": "preenchimento_caderno",
    "IN_PRESENCA_": "presenca",
    "ID_CADERNO_": "caderno",
    "ID_BLOCO_1_": "bloco_1",
    "ID_BLOCO_2_": "bloco_2",
    "TX_RESP_BLOCO1_": "respostas_bloco_1",
    "TX_RESP_BLOCO2_": "respostas_bloco_2",
    "IN_PROFICIENCIA_": "indicador_proficiencia",
    "IN_AMOSTRA": "amostra",
    "ESTRATO": "estrato",
    "PESO_ALUNO_": "peso_aluno",
    "PROFICIENCIA_": "proficiencia",
    "ERRO_PADRAO_": "desvio_padrao",
    "PROFICIENCIA__SAEB": "proficiencia_saeb",
    "ERRO_PADRAO__SAEB": "desvio_padrao_saeb",
    "IN_PREENCHIMENTO_QUESTIONARIO": "preenchimento_questionario",
    "TX_RESP_Q01": "sexo",
    "TX_RESP_Q04": "raca_cor",
    "TX_RESP_Q02": "idade",
    # complementar
    "TX_RESP_Q05a": "possui_necessidade_especial_1",
    "TX_RESP_Q05b": "possui_necessidade_especial_2",
    "TX_RESP_Q05c": "possui_necessidade_especial_3",
    "TX_RESP_Q07a": "mora_mae",
    "TX_RESP_Q07b": "mora_pai",
    # complementar
    "TX_RESP_Q07c": "mora_avos_1",
    "TX_RESP_Q07d": "mora_avos_2",
    "TX_RESP_Q07e": "mora_outros_parentes",
    "TX_RESP_Q06": "quantidade_pessoas_domicilio",
    "IN_INSE": "indicador_inse",
    "INSE_ALUNO": "inse",
    "NU_TIPO_NIVEL_INSE": "nivel_inse",
    "PESO_ALUNO_INSE": "peso_inse",
    "TX_RESP_Q11a": "possui_moradia_rua_urbanizada",
    "TX_RESP_Q11b": "possui_agua_encanada",
    "TX_RESP_Q11c": "possui_eletrecidade",
    "TX_RESP_Q12c": "possui_casa_dormitorio",
    "TX_RESP_Q13c": "possui_casa_quarto_individual",
    "TX_RESP_Q12e": "possui_casa_banheiro",
    "TX_RESP_Q13i": "possui_casa_garagem",
    "TX_RESP_Q12f": "possui_automovel",
    "TX_RESP_Q12a": "possui_geladeira",
    "TX_RESP_Q13h": "possui_freezer",
    "TX_RESP_Q13e": "possui_microondas",
    "TX_RESP_Q13g": "possui_maquina_lavar_roupa",
    "TX_RESP_Q13f": "possui_aspirador_po",
    "TX_RESP_Q12d": "possui_tv",
    "TX_RESP_Q13a": "possui_tv_assinatura",
    "TX_RESP_Q13b": "possui_internet",
    "TX_RESP_Q12b": "possui_computador",
    "TX_RESP_Q13d": "possui_escrivaninha",
    "TX_RESP_Q08": "escolaridade_mae",
    "TX_RESP_Q09": "escolaridade_pai",
    "TX_RESP_Q10b": "responsaveis_conversam_escola",
    "TX_RESP_Q10a": "responsaveis_leem",
    "TX_RESP_Q10d": "responsaveis_incentivam_realizacao_licao_casa",
    "TX_RESP_Q10c": "responsaveis_incentivam_estudos",
    "TX_RESP_Q10e": "responsaveis_incentivam_comparecer_aulas",
    "TX_RESP_Q10f": "responsaveis_comparecem_reuniao_pais",
    "TX_RESP_Q14": "tempo_chegada_escola",
    "TX_RESP_Q16": "forma_chegada_escola",
    # complementar
    "TX_RESP_Q15a": "transporte_escolar_1",
    "TX_RESP_Q15b": "transporte_escolar_2",
    "TX_RESP_Q17": "idade_entrada_escola",
    "TX_RESP_Q18": "rede_ef",
    "TX_RESP_Q19": "reprovacao",
    "TX_RESP_Q20": "evasao_escolar_ate_final_ano",
    "TX_RESP_Q21e": "tempo_lazer",
    "TX_RESP_Q21b": "tempo_cursos",
    "TX_RESP_Q21c": "tempo_trabalho_domestico",
    "TX_RESP_Q21a": "tempo_estudos",
}


upstream_5ano_columns = bd.read_sql(
    "select * from basedosdados-dev.br_inep_saeb_staging.aluno_ef_5ano limit 0",
    billing_project_id="basedosdados-dev",
).columns.to_list()

df_wide_5ano: pd.DataFrame = df_wide_5ano.rename(
    columns=renames_5ano, errors="raise"
)[[*list(renames_5ano.values()), *["disciplina"]]]  # type: ignore


df_wide_5ano["possui_necessidade_especial"] = df_wide_5ano[
    [
        "possui_necessidade_especial_1",
        "possui_necessidade_especial_2",
        "possui_necessidade_especial_3",
    ]
].apply(lambda v: possui_necessidade_especial(v[0], v[1], v[2]), axis=1)

df_wide_5ano = df_wide_5ano.drop(
    columns=[
        "possui_necessidade_especial_1",
        "possui_necessidade_especial_2",
        "possui_necessidade_especial_3",
    ]
)


df_wide_5ano["transporte_escolar"] = df_wide_5ano[
    [
        "transporte_escolar_1",
        "transporte_escolar_2",
    ]
].apply(lambda v: transporte_escolar(v[0], v[1]), axis=1)

df_wide_5ano = df_wide_5ano.drop(
    columns=[
        "transporte_escolar_1",
        "transporte_escolar_2",
    ]
)


df_wide_5ano["mora_avos"] = df_wide_5ano[
    [
        "mora_avos_1",
        "mora_avos_2",
    ]
].apply(lambda v: mora_avos(v[0], v[1]), axis=1)


df_wide_5ano = df_wide_5ano.drop(
    columns=[
        "mora_avos_1",
        "mora_avos_2",
    ]
)

df_wide_5ano["sigla_uf"] = (
    df_wide_5ano["id_uf"]
    .astype("string")
    .replace({i["id_uf"]: i["sigla"] for i in bd_dir_uf.to_dict("records")})
)

df_wide_5ano = df_wide_5ano.drop(columns=["id_uf"])

missing_cols_5ano: list[str] = [
    i for i in upstream_5ano_columns if i not in df_wide_5ano.columns
]

df_wide_5ano[missing_cols_5ano] = None

df_wide_5ano["rede"].unique()
df_wide_5ano["escola_publica"].unique()

df_wide_5ano["rede"] = df_wide_5ano["escola_publica"]

df_wide_5ano = df_wide_5ano[upstream_5ano_columns]  # type: ignore

for (ano, sigla_uf), df_ano_uf_disc in df_wide_5ano.groupby(  # type: ignore
    ["ano", "sigla_uf"]
):
    path = OUTPUT / "aluno_ef_5ano" / f"ano={ano}" / f"sigla_uf={sigla_uf}"
    path.mkdir(parents=True, exist_ok=True)
    df_ano_uf_disc.drop(columns=["ano", "sigla_uf"]).to_csv(
        path / "data.csv", index=False
    )

tb_5ano = bd.Table(dataset_id="br_inep_saeb", table_id="aluno_ef_5ano")

tb_5ano.create(
    path=OUTPUT / "aluno_ef_5ano",
    if_table_exists="replace",
    if_storage_data_exists="replace",
)

# 9 ano

df_aluno_ef_9ano = pd.read_csv(
    INPUT / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_9EF.csv", sep=";"
)

value_vars_9ano = [
    i for i in df_aluno_ef_9ano.columns if "MT" in i or "LP" in i
]

id_vars_9ano = list(set(df_aluno_ef_9ano.columns) - set(value_vars_9ano))

df_wide_9ano = pd.concat(
    batch_process(
        df_aluno_ef_9ano, value_vars=value_vars_9ano, id_vars=id_vars_9ano
    )
)


renames_9ano = {
    "ID_SAEB": "ano",
    "ID_UF": "id_uf",
    "ID_REGIAO": "id_regiao",
    "ID_MUNICIPIO": "id_municipio",
    "ID_AREA": "area",
    "ID_ESCOLA": "id_escola",
    "IN_PUBLICA": "escola_publica",
    "ID_LOCALIZACAO": "localizacao",
    "ID_TURMA": "id_turma",
    "ID_SERIE": "serie",
    "ID_ALUNO": "id_aluno",
    "IN_SITUACAO_CENSO": "situacao_censo",
    "IN_PREENCHIMENTO_": "preenchimento_caderno",
    "IN_PRESENCA_": "presenca",
    "ID_CADERNO_": "caderno",
    "ID_BLOCO_1_": "bloco_1",
    "ID_BLOCO_2_": "bloco_2",
    "TX_RESP_BLOCO1_": "respostas_bloco_1",
    "TX_RESP_BLOCO2_": "respostas_bloco_2",
    "IN_PROFICIENCIA_": "indicador_proficiencia",
    "IN_AMOSTRA": "amostra",
    "ESTRATO": "estrato",
    "PESO_ALUNO_": "peso_aluno",
    "PROFICIENCIA_": "proficiencia",
    "ERRO_PADRAO_": "erro_padrao",
    "PROFICIENCIA__SAEB": "proficiencia_saeb",
    "ERRO_PADRAO__SAEB": "erro_padrao_saeb",
    "IN_PREENCHIMENTO_QUESTIONARIO": "preenchimento_questionario",
    "TX_RESP_Q01": "sexo",
    "TX_RESP_Q04": "raca_cor",
    "TX_RESP_Q02": "faixa_etaria",
    # complementar
    "TX_RESP_Q05a": "possui_necessidade_especial_1",
    "TX_RESP_Q05b": "possui_necessidade_especial_2",
    "TX_RESP_Q05c": "possui_necessidade_especial_3",
    "TX_RESP_Q07a": "mora_mae",
    "TX_RESP_Q07b": "mora_pai",
    # complementar
    "TX_RESP_Q07c": "mora_avos_1",
    "TX_RESP_Q07d": "mora_avos_2",
    "TX_RESP_Q07e": "mora_outros_parentes",
    "TX_RESP_Q03": "idioma_domicilio",
    "TX_RESP_Q06": "quantidade_pessoas_domicilio",
    "IN_INSE": "indicador_inse",
    "INSE_ALUNO": "inse",
    "NU_TIPO_NIVEL_INSE": "nivel_inse",
    "PESO_ALUNO_INSE": "peso_inse",
    "TX_RESP_Q21d": "possui_trabalho",
    "TX_RESP_Q11a": "possui_moradia_rua_urbanizada",
    "TX_RESP_Q11b": "possui_agua_encanada",
    "TX_RESP_Q11c": "possui_eletrecidade",
    "TX_RESP_Q12c": "possui_casa_dormitorio",
    "TX_RESP_Q13c": "possui_casa_quarto_individual",
    "TX_RESP_Q12e": "possui_casa_banheiro",
    "TX_RESP_Q13i": "possui_casa_garagem",
    "TX_RESP_Q12f": "possui_automovel",
    "TX_RESP_Q12a": "possui_geladeira",
    "TX_RESP_Q13h": "possui_freezer",
    "TX_RESP_Q13e": "possui_microondas",
    "TX_RESP_Q13g": "possui_maquina_lavar_roupa ",
    "TX_RESP_Q13f": "possui_aspirador_po",
    "TX_RESP_Q12d": "possui_tv",
    "TX_RESP_Q13a": "possui_tv_assinatura",
    "TX_RESP_Q13b": "possui_internet",
    "TX_RESP_Q12b": "possui_computador",
    "TX_RESP_Q12g": "possui_celular",
    "TX_RESP_Q13d": "possui_escrivaninha",
    "TX_RESP_Q08": "escolaridade_mae",
    "TX_RESP_Q09": "escolaridade_pai",
    "TX_RESP_Q10b": "responsaveis_conversam_escola",
    "TX_RESP_Q10a": "responsaveis_leem",
    "TX_RESP_Q10d": "responsaveis_incentivam_realizacao_licao_casa",
    "TX_RESP_Q10c": "responsaveis_incentivam_estudos",
    "TX_RESP_Q10e": "responsaveis_incentivam_comparecer_aulas",
    "TX_RESP_Q10f": "responsaveis_comparecem_reuniao_pais",
    "TX_RESP_Q14": "tempo_chegada_escola",
    "TX_RESP_Q16": "forma_chegada_escola",
    "TX_RESP_Q15a": "transporte_escolar_1",
    "TX_RESP_Q15b": "transporte_escolar_2",
    "TX_RESP_Q17": "idade_entrada_escola",
    "TX_RESP_Q19": "reprovacao",
    "TX_RESP_Q20": "evasao_escolar_ate_final_ano",
    "TX_RESP_Q21e": "tempo_lazer",
    "TX_RESP_Q21b": "tempo_cursos",
    "TX_RESP_Q21c": "tempo_trabalho_domestico",
    "TX_RESP_Q21a": "tempo_estudos",
}

upstream_9ano_columns: list[str] = bd.read_sql(
    "select * from basedosdados-dev.br_inep_saeb_staging.aluno_ef_9ano limit 0",
    billing_project_id="basedosdados-dev",
).columns.to_list()

df_wide_9ano: pd.DataFrame = df_wide_9ano.rename(
    columns=renames_9ano, errors="raise"
)[[*list(renames_9ano.values()), *["disciplina"]]]  # type: ignore

df_wide_9ano["possui_necessidade_especial"] = df_wide_9ano[
    [
        "possui_necessidade_especial_1",
        "possui_necessidade_especial_2",
        "possui_necessidade_especial_3",
    ]
].apply(lambda v: possui_necessidade_especial(v[0], v[1], v[2]), axis=1)

df_wide_9ano = df_wide_9ano.drop(
    columns=[
        "possui_necessidade_especial_1",
        "possui_necessidade_especial_2",
        "possui_necessidade_especial_3",
    ]
)

df_wide_9ano["transporte_escolar"] = df_wide_9ano[
    [
        "transporte_escolar_1",
        "transporte_escolar_2",
    ]
].apply(lambda v: transporte_escolar(v[0], v[1]), axis=1)

df_wide_9ano = df_wide_9ano.drop(
    columns=[
        "transporte_escolar_1",
        "transporte_escolar_2",
    ]
)

df_wide_9ano["mora_avos"] = df_wide_9ano[
    [
        "mora_avos_1",
        "mora_avos_2",
    ]
].apply(lambda v: mora_avos(v[0], v[1]), axis=1)


df_wide_9ano = df_wide_9ano.drop(
    columns=[
        "mora_avos_1",
        "mora_avos_2",
    ]
)

df_wide_9ano["sigla_uf"] = (
    df_wide_9ano["id_uf"]
    .astype("string")
    .replace({i["id_uf"]: i["sigla"] for i in bd_dir_uf.to_dict("records")})
)

df_wide_9ano = df_wide_9ano.drop(columns=["id_uf"])

missing_cols_9ano = [
    i for i in upstream_9ano_columns if i not in df_wide_9ano.columns
]

df_wide_9ano[missing_cols_9ano] = None

df_wide_9ano["rede"].unique()  # type: ignore
df_wide_9ano["escola_publica"].unique()  # type: ignore

df_wide_9ano["rede"] = df_wide_9ano["escola_publica"]

df_wide_9ano = df_wide_9ano[upstream_9ano_columns]  # type: ignore

for (ano, sigla_uf), df_ano_uf_disc in df_wide_9ano.groupby(  # type: ignore
    ["ano", "sigla_uf"]
):
    path = OUTPUT / "aluno_ef_9ano" / f"ano={ano}" / f"sigla_uf={sigla_uf}"
    path.mkdir(parents=True, exist_ok=True)
    df_ano_uf_disc.drop(columns=["ano", "sigla_uf"]).to_csv(
        path / "data.csv", index=False
    )

tb_9ano = bd.Table(dataset_id="br_inep_saeb", table_id="aluno_ef_9ano")

tb_9ano.create(
    path=OUTPUT / "aluno_ef_9ano",
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
