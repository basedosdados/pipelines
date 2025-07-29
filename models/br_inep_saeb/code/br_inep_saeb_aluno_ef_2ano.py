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
    # "CO_CONCEITO_COESAO": "",
    "ID_SAEB": "ano",
    "ID_ESCOLA": "id_escola",
    "CO_CONCEITO_SEGMENTACAO": "conceito_segmentacao",
    # "IN_ALFABETIZADO": "",
    "CO_RESPOSTA_TEXTO": "resposta_texto",
    "ID_REGIAO": "id_regiao",
    "ID_UF": "id_uf",
    # "CO_CONCEITO_PONTUACAO": "",
    "ID_SERIE": "serie",
    "IN_SITUACAO_CENSO": "situacao_censo",
    # "CO_CONCEITO_SEQUENCIA": "",
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


bd_dir_uf = bd.read_sql(
    "select id_uf, sigla from `basedosdados-dev.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

upstream_2ano_columns = bd.read_sql(
    "select * from basedosdados-dev.br_inep_saeb_staging.aluno_ef_2ano limit 0",
    billing_project_id="basedosdados-dev",
).columns


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
# https://docs.google.com/spreadsheets/d/179zzpF8rcCFHxdaN1QAmA52xvHAVxYZsgd1urGwDbdw/edit?gid=1038748650#gid=1038748650

df_aluno_ef_5ano = pd.read_csv(
    INPUT / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_5EF.csv", sep=";"
)

value_vars_5ano = [
    i for i in df_aluno_ef_5ano.columns if "MT" in i or "LP" in i
]

id_vars_5ano = list(set(df_aluno_ef_5ano.columns) - set(value_vars_5ano))


def process_5ano(df: pd.DataFrame):
    batch_size = 100_000

    result = []

    for _, batch_df in enumerate(np.array_split(df, len(df) // batch_size)):
        assert isinstance(batch_df, pd.DataFrame)

        batch_df_long = batch_df.melt(
            id_vars=id_vars_5ano, value_vars=value_vars_5ano
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


df_wide_5ano = pd.concat(process_5ano(df_aluno_ef_5ano))

# df_wide_5ano.to_csv("wide_5ano.csv", index=False)

df_wide_5ano = pd.read_csv("wide_5ano.csv")

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

df_wide_5ano = df_wide_5ano.rename(columns=renames_5ano, errors="raise")[
    renames_5ano.values()
]


def possui_necessidade_especial(a: str, b: str, c: str) -> str:
    empty = ["*", "."]
    values = [a, b, c]

    for value in values:
        if value not in empty:
            return value

    return a


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


def transporte_escolar(a: str, b: str) -> str:
    empty = ["*", "."]
    values = [a, b]

    for value in values:
        if value not in empty:
            return value

    return a


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

df_wide_5ano["sigla_uf"] = (
    df_wide_5ano["id_uf"]
    .astype("string")
    .replace({i["id_uf"]: i["sigla"] for i in bd_dir_uf.to_dict("records")})
)

df_wide_5ano = df_wide_5ano.drop(columns=["id_uf"])

missing_cols_5ano = [
    i for i in upstream_5ano_columns if i not in df_wide_5ano.columns
]

df_wide_5ano[missing_cols_5ano] = None

df_wide_5ano = df_wide_5ano[upstream_5ano_columns]


def to_partitions(
    data: pd.DataFrame, partition_columns: list[str], savepath: Path
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, pd.DataFrame):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


save_path = OUTPUT / "aluno_ef_5ano"
save_path.mkdir(exist_ok=True)

to_partitions(df_wide_5ano, ["ano", "sigla_uf"], save_path)

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

id_vars_9ano = list(set(df_aluno_ef_9ano.columns) - set(value_vars_5ano))
