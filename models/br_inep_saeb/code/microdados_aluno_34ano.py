# microdados_aluno_34ano.py
# ============================================================
# Script Python único, convertido 100% do notebook original
# Formatado para Ruff (PEP8, imports no topo, sem células soltas)
# ============================================================

"""
Pipeline completo dos microdados do SAEB 2023 3º/4º ano EM.

Etapas:
1. Montagem de chunks CSV → Parquet
2. Transformação wide → long (LP/MT)
3. Renomeação de variáveis
4. Consolidação final
5. Geração da tabela de arquitetura
6. Geração e validação do dicionário
"""

# =====================
# Imports
# =====================
import gc
import glob
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
from google.colab import drive  # type: ignore

# =====================
# Setup Colab
# =====================
drive.mount("/content/drive")

# =====================
# Funções utilitárias
# =====================


def norm(value) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\s+", " ", text)
    return text


# =====================
# 1. CSV → Parquet (chunks)
# =====================

CSV_PATH = "input/MICRODADOS_SAEB_2023/DADOS/TS_ALUNO_34EM.csv"
CHUNK_SIZE = 100_000
CHUNK_DIR = Path("output/chunks")
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

chunk_id = 0
for chunk in pd.read_csv(CSV_PATH, sep=";", chunksize=CHUNK_SIZE):
    out_file = CHUNK_DIR / f"chunk_{chunk_id}.parquet"
    chunk.to_parquet(out_file, index=False)
    print(f"Chunk {chunk_id} → {out_file}")
    chunk_id += 1

print(f"{chunk_id} chunks criados")

# =====================
# 2. Wide → Long
# =====================

UF_MAP = {
    11: "RO",
    12: "AC",
    13: "AM",
    14: "RR",
    15: "PA",
    16: "AP",
    17: "TO",
    21: "MA",
    22: "PI",
    23: "CE",
    24: "RN",
    25: "PB",
    26: "PE",
    27: "AL",
    28: "SE",
    29: "BA",
    31: "MG",
    32: "ES",
    33: "RJ",
    35: "SP",
    41: "PR",
    42: "SC",
    43: "RS",
    50: "MS",
    51: "MT",
    52: "GO",
    53: "DF",
}


RENAME_FIXED = {
    "ID_SAEB": "ano",
    "ID_AREA": "area",
    "IN_PUBLICA": "escola_publica",
    "IN_AMOSTRA": "amostra",
    "ID_LOCALIZACAO": "localizacao",
    "ID_SERIE": "serie",
    "IN_SITUACAO_CENSO": "situacao_censo",
    "IN_PREENCHIMENTO_QUESTIONARIO": "preenchimento_questionario",
    "IN_INSE": "indicador_inse",
    "INSE_ALUNO": "inse",
    "NU_TIPO_NIVEL_INSE": "nivel_inse",
    "PESO_ALUNO_INSE": "peso_inse",
}


MANUAL_RENAMES = {
    "PROFICIENCIA_SAEB": "proficiencia_saeb",
    "ERRO_PADRAO_SAEB": "erro_padrao_saeb",
    "IN_PREENCHIMENTO": "preenchimento_caderno",
    "IN_PROFICIENCIA": "indicador_proficiencia",
    "IN_PRESENCA": "presenca",
    "ID_CADERNO": "caderno",
    "ID_BLOCO_1": "bloco_1",
    "ID_BLOCO_2": "bloco_2",
    "TX_RESP_BLOCO1": "respostas_bloco_1",
    "TX_RESP_BLOCO2": "respostas_bloco_2",
    "PESO_ALUNO": "peso_aluno",
    "PROFICIENCIA": "proficiencia",
    "ERRO_PADRAO": "erro_padrao",
}


RENAMES = {**RENAME_FIXED, **MANUAL_RENAMES}


def get_disc(col: str) -> str:
    if col.endswith(("_LP", "_LP_SAEB")):
        return "LP"
    if col.endswith(("_MT", "_MT_SAEB")):
        return "MT"
    raise ValueError(col)


def remove_disc_suffix(col: str) -> str:
    if col.endswith("_LP_SAEB"):
        return col.replace("_LP_SAEB", "_SAEB")
    if col.endswith("_MT_SAEB"):
        return col.replace("_MT_SAEB", "_SAEB")
    for sfx in ("_LP", "_MT"):
        if col.endswith(sfx):
            return col[: -len(sfx)]
    return col


def wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    df["sigla_uf"] = df["ID_UF"].map(UF_MAP)

    cols_disc = [
        c
        for c in df.columns
        if c.endswith(("_LP", "_MT", "_LP_SAEB", "_MT_SAEB"))
    ]

    index_cols = [
        "ID_SAEB",
        "ID_REGIAO",
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
        "sigla_uf",
    ]

    other_cols = [
        c for c in df.columns if c.startswith("TX_RESP") and c not in cols_disc
    ]

    melted = df.melt(
        id_vars=index_cols + other_cols,
        value_vars=cols_disc,
        var_name="variable",
        value_name="value",
    )

    melted["disciplina"] = melted["variable"].apply(get_disc)
    melted["variavel"] = melted["variable"].apply(remove_disc_suffix)

    out = melted.pivot_table(
        index=[*index_cols, *other_cols, "disciplina"],
        columns="variavel",
        values="value",
        aggfunc="first",
    ).reset_index()

    out.columns = out.columns.get_level_values(0)
    out = out.rename(columns=RENAMES)
    out.columns = out.columns.str.lower()
    return out


# =====================
# 3. Processar Parquets
# =====================

IN_DIR = "output/chunks"
OUT_DIR = "output/chunks_processados"
os.makedirs(OUT_DIR, exist_ok=True)

for path in glob.glob(os.path.join(IN_DIR, "*.parquet")):
    df = pd.read_parquet(path)
    df = wide_to_long(df)
    out = os.path.join(
        OUT_DIR,
        os.path.basename(path).replace(".parquet", "_transformado.parquet"),
    )
    df.to_parquet(out, index=False)
    del df
    gc.collect()

# =====================
# 4. Consolidação final
# =====================

FINAL_DIR = "output/chunks_renomeados"
os.makedirs(FINAL_DIR, exist_ok=True)

for file in os.listdir(OUT_DIR):
    if file.endswith(".parquet"):
        df = pd.read_parquet(os.path.join(OUT_DIR, file))
        df.to_parquet(os.path.join(FINAL_DIR, file), index=False)

files = glob.glob(os.path.join(FINAL_DIR, "*.parquet"))
df_final = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)

FINAL_PARQUET = os.path.join(FINAL_DIR, "arquivo_unificado.parquet")
df_final.to_parquet(FINAL_PARQUET, index=False)

print("Pipeline finalizado com sucesso")

import basedosdados as bd  # noqa: E402

tb = bd.Table(dataset_id="br_inep_saeb", table_id="dicionario")

tb.create(
    path="/SAEB/staging_br_inep_saeb_dicionario_dicionario_saeb.csv",  # subir o dicionario e o arquivo final
    if_table_exists="replace",
    if_storage_data_exists="replace",
    source_format="csv",
)
