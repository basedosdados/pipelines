# -*- coding: utf-8 -*-
import polars as pl
import os
import basedosdados as bd
from databasers_utils import TableArchitecture
import zipfile

ROOT = os.path.join("models", "br_inep_saeb")
INPUT = os.path.join(ROOT, "input")
TMP = os.path.join(ROOT, "tmp")
OUTPUT = os.path.join(ROOT, "output", "aluno_ef_9ano")

ZIP_URL = "https://download.inep.gov.br/microdados/microdados_saeb_2021_ensino_fundamental_e_medio.zip"

ZIP_FILE = os.path.join(INPUT, os.path.basename(ZIP_URL))

os.system(
    f"wget {ZIP_URL} --no-check-certificate -o {INPUT}/{os.path.basename(ZIP_URL)}"
)

with zipfile.ZipFile(ZIP_FILE) as z:
    print(z.namelist())

with zipfile.ZipFile(ZIP_FILE) as z:
    z.extract("DADOS/TS_ALUNO_9EF.csv", TMP)

# Apenas Roraima (RR)
df = pl.read_csv(os.path.join(TMP, "DADOS/TS_ALUNO_9EF.csv"), separator=";").filter(
    pl.col("ID_UF") == 14
)

csv_columns = df.columns

arch = TableArchitecture(
    "br_inep_saeb",
    {
        "aluno_ef_9ano": "https://docs.google.com/spreadsheets/d/1KLkvX8z9AKIe4iM5EeahVBCVUjikNvHJWfT-qTjM2IU/edit?gid=0#gid=0",
    },
)

tables_arch = arch.tables()

arch_ef_9ano = tables_arch["aluno_ef_9ano"]

cols_disciplina = [
    i
    for i in csv_columns
    if i.endswith("LP") or i.endswith("MT") or i.endswith("CH") or i.endswith("CN")
]

index_cols = [
    "ID_SAEB",
    "ID_REGIAO",
    "ID_UF",
    "ID_MUNICIPIO",
    "ID_AREA",
    "ID_ESCOLA",
    "IN_PUBLICA",
    "ID_LOCALIZACAO",
    "ID_TURMA",
    "ID_SERIE",
    "ID_ALUNO",
    "IN_SITUACAO_CENSO",
    "IN_AMOSTRA",
    "ESTRATO",
    "IN_PREENCHIMENTO_QUESTIONARIO",
    "IN_INSE",
    "INSE_ALUNO",
    "NU_TIPO_NIVEL_INSE",
    "PESO_ALUNO_INSE",
]

on = [
    "PROFICIENCIA_LP_SAEB",
    "PROFICIENCIA_MT_SAEB",
    "PROFICIENCIA_CH_SAEB",
    "PROFICIENCIA_CN_SAEB",
    "ERRO_PADRAO_LP_SAEB",
    "ERRO_PADRAO_MT_SAEB",
    "ERRO_PADRAO_CH_SAEB",
    "ERRO_PADRAO_CN_SAEB",
    *cols_disciplina,
]


def find_disc(value: str) -> str:
    """
    Returns two characters identifying the subject from a variable

    Parameters
    ----------
    value
        Variable name

    Examples
    --------
    >>> find_disc("PROFICIENCIA_CH_SAEB")
    >>> "CH"
    """
    last_two_char = value[-2:]
    if last_two_char not in ["LP", "MT", "CH", "CN"]:
        return value.split("_")[-2]
    return last_two_char


def renames_variables(value: tuple[str, str]) -> str:
    """
    Rename variables using subject

    Parameters
    ----------
    value:
        Tuple (two values), lhs is variable name and rhs is subject

    Examples
    --------
    >>> renames_variables(("PROFICIENCIA_CH_SAEB", "CH"))
    >>> "PROFICIENCIA__SAEB"
    """
    variable, disc = value
    parts = variable.split("_")

    if disc in parts:
        return "_".join([i for i in parts if i not in disc])

    return variable


other_index_cols = [
    i
    for i in csv_columns
    if i.startswith("TX_RESP") and i.split("_")[-1] not in ["LP", "MT", "CH", "CN"]
]


def wide_to_long(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert a DataFrame from wide to long format
    """
    return (
        df.unpivot(on=on, index=[*index_cols, *other_index_cols])
        .with_columns(
            pl.col("variable")
            .map_elements(lambda v: find_disc(v), return_dtype=pl.String)
            .alias("disciplina"),
        )
        .with_columns(
            pl.struct(["variable", "disciplina"]).map_elements(
                lambda cols: renames_variables((cols["variable"], cols["disciplina"])),
                return_dtype=pl.String,
            )
        )
        .pivot(
            on="variable",
            values="value",
            index=[*index_cols, *other_index_cols, "disciplina"],
        )
    )


manual_renames = {
    "PROFICIENCIA_SAEB": "proficiencia_saeb",
    "ERRO_PADRAO_SAEB": "erro_padrao_saeb",
    "IN_PREENCHIMENTO": "preenchimento_caderno",
    "IN_PRESENCA": "presenca",
    "ID_CADERNO": "caderno",
    "ID_BLOCO_1": "bloco_1",
    "ID_BLOCO_2": "bloco_2",
    "ID_BLOCO_3": "bloco_3",
    "NU_BLOCO_1_ABERTA": "bloco_1_aberto",
    "NU_BLOCO_2_ABERTA": "bloco_2_aberto",
    "TX_RESP_BLOCO1": "respostas_bloco_1",
    "TX_RESP_BLOCO2": "respostas_bloco_2",
    "TX_RESP_BLOCO3": "respostas_bloco_3",
    "CO_CONCEITO_Q1": "conceito_q1",
    "CO_CONCEITO_Q2": "conceito_q2",
    "IN_PROFICIENCIA": "indicador_proficiencia",
    "PESO_ALUNO": "peso_aluno",
    "PROFICIENCIA": "proficiencia",
    "ERRO_PADRAO": "erro_padrao",
}

tb_aluno_ef_9ano = bd.Table("br_inep_saeb", table_id="aluno_ef_9ano")

bq_cols = tb_aluno_ef_9ano._get_columns_from_bq()

cols_dict = dict([(i["name"], i["type"]) for i in bq_cols["columns"]])

common_renames = {
    i["original_name_2021"]: i["name"]
    for i in arch_ef_9ano.loc[arch_ef_9ano["original_name_2021"] != ""][
        ["name", "original_name_2021"]
    ].to_dict("records")
    if i["original_name_2021"] in csv_columns
}

df = (
    wide_to_long(df)
    .rename({**common_renames, **manual_renames})
    .with_columns(pl.lit("RR").alias("sigla_uf"))
    .drop(["ID_UF"])
)


empty_cols_to_add = [i for i in cols_dict.keys() if i not in df.columns]

len(empty_cols_to_add)

df = (
    df.with_columns([pl.lit(None).alias(col) for col in empty_cols_to_add])
    .with_columns([pl.col(col_name).cast(pl.String) for col_name in cols_dict.keys()])
    .select(*cols_dict.keys())
    .filter(pl.col("disciplina") == "MT")
)

assert len(df.columns) == len(bq_cols["columns"])

PARTITION_DIR = os.path.join(OUTPUT, "ano=2021", "sigla_uf=RR")

os.makedirs(PARTITION_DIR, exist_ok=True)

df.write_csv(os.path.join(PARTITION_DIR, "microdados_mt.csv"))

tb_aluno_ef_9ano.create(
    OUTPUT,
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
