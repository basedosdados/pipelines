"""
Esse script atualiza as seguintes tabelas do dataset br_inep_educacao_especial
para 2024.

- etapa_ensino
- faixa_etaria
- localizacao
- tempo_ensino
- sexo_raca_cor
- tipo_deficiencia
"""

import os
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

ROOT = Path("models") / "br_inep_educacao_especial"
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_2024.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "2024.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2024.zip") as z:
    z.extractall(INPUT)


def read_sheet(sheet_name: str, skiprows: int = 9) -> pd.DataFrame:
    return pd.read_excel(
        INPUT
        / "sinopse_estatistica_censo_escolar_2024"
        / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx",
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


bd_dir = bd.read_sql(
    "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

# Etapa ensino

df_etapa_ensino_classes_comuns = read_sheet("Classes Comuns 1.40")
df_etapa_ensino_classes_exclusisas = read_sheet("Classes Exclusivas 1.46")

RENAMES_ETAPA_ENSINO = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Creche": "Educação Infantil - Creche",
    "Pré-Escola": "Educação Infantil - Pré-Escola",
    "Anos Iniciais7": "Ensino Fundamental - Anos Iniciais",
    "Anos Finais8": "Ensino Fundamental - Anos Finais",
    "Ensino Médio Propedêutico ": "Ensino Médio - Ensino Médio Propedêutico",
    "Ensino Médio Normal/Magistério": "Ensino Médio Normal - Magistério",
    "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado (Ensino Médio Integrado)",
    "Associada ao Ensino Médio12": "Educação Profissional - Associada ao Ensino Médio",
    "Curso Técnico Concomitante": "Educação Profissional - Curso Técnico Concomitante",
    "Curso Técnico Subsequente": "Educação Profissional - Curso Técnico Subsequente",
    "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
    "Curso FIC Integrado na Modalidade EJA14": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
    "Ensino Fundamental16": "EJA - Ensino Fundamental",
    "Ensino Médio17": "EJA - Ensino Médio",
}

df_etapa_ensino_classes_comuns = (
    df_etapa_ensino_classes_comuns.rename(
        columns=RENAMES_ETAPA_ENSINO, errors="raise"
    )[RENAMES_ETAPA_ENSINO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_etapa_ensino_classes_exclusisas = (
    df_etapa_ensino_classes_exclusisas.rename(
        columns=RENAMES_ETAPA_ENSINO, errors="raise"
    )[RENAMES_ETAPA_ENSINO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_etapa_ensino = (
    pd.concat(
        [df_etapa_ensino_classes_exclusisas, df_etapa_ensino_classes_comuns]
    )
    .assign(
        sigla_uf=lambda d: d["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
        quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
            "Int64"
        ),
    )
    .rename(columns={"variable": "etapa_ensino"})[
        [
            "id_municipio",
            "tipo_classe",
            "etapa_ensino",
            "quantidade_matricula",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_etapa_ensino.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "etapa_ensino" / "ano=2024" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

# Faixa etaria

df_faixa_etaria_classes_comuns = read_sheet("1.43", skiprows=7)
df_faixa_etaria_classes_exclusisas = read_sheet("1.49", skiprows=7)

RENAMES_FAIXA_ETARIA = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Até 14 anos": "Até 14 anos",
    "15 a 17 anos": "15 a 17 anos",
    "18 a 24 anos": "18 a 24 anos",
    "25 a 29 anos": "25 a 29 anos",
    "30 a 34 anos": "30 a 34 anos",
    "35 anos ou mais ": "35 anos ou mais",
}

df_faixa_etaria_classes_comuns = (
    df_faixa_etaria_classes_comuns.rename(
        columns=RENAMES_FAIXA_ETARIA, errors="raise"
    )[RENAMES_FAIXA_ETARIA.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_faixa_etaria_classes_exclusisas = (
    df_faixa_etaria_classes_exclusisas.rename(
        columns=RENAMES_FAIXA_ETARIA, errors="raise"
    )[RENAMES_FAIXA_ETARIA.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_faixa_etaria = (
    pd.concat(
        [df_faixa_etaria_classes_exclusisas, df_faixa_etaria_classes_comuns]
    )
    .assign(
        sigla_uf=lambda d: d["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
        quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
            "Int64"
        ),
    )
    .rename(columns={"variable": "etapa_ensino"})[
        [
            "id_municipio",
            "tipo_classe",
            "etapa_ensino",
            "quantidade_matricula",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_faixa_etaria.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "faixa_etaria" / "ano=2024" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

# Localizacao

df_localizacao_classes_comuns = read_sheet("1.41", skiprows=8)
df_localizacao_classes_exclusisas = read_sheet("1.47", skiprows=8)

RENAMES_LOCALIZACAO = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Federal": "urbana_federal",
    "Estadual": "urbana_estadual",
    "Municipal": "urbana_municipal",
    "Privada": "urbana_privada",
    "Federal.1": "rural_federal",
    "Estadual.1": "rural_estadual",
    "Municipal.1": "rural_municipal",
    "Privada.1": "rural_privada",
}

df_localizacao_classes_comuns = (
    df_localizacao_classes_comuns.rename(
        columns=RENAMES_LOCALIZACAO, errors="raise"
    )[RENAMES_LOCALIZACAO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("rural") or c.startswith("urbana")
            ],
            var_name="localizacao",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_localizacao_classes_exclusisas = (
    df_localizacao_classes_exclusisas.rename(
        columns=RENAMES_LOCALIZACAO, errors="raise"
    )[RENAMES_LOCALIZACAO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("rural") or c.startswith("urbana")
            ],
            var_name="localizacao",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_localizacao = pd.concat(
    [df_localizacao_classes_exclusisas, df_localizacao_classes_comuns]
).assign(
    sigla_uf=lambda d: d["uf"]
    .apply(lambda uf: uf.strip())
    .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
    quantidade_matricula=lambda d: d["quantidade_matricula"].astype("Int64"),
    rede=lambda d: d["localizacao"].apply(
        lambda v: v.split("_")[-1].title().strip()
    ),
    localizacao=lambda d: d["localizacao"].apply(
        lambda v: v.split("_")[0].title().strip()
    ),
)[
    [
        "id_municipio",
        "tipo_classe",
        "rede",
        "localizacao",
        "quantidade_matricula",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_localizacao.groupby("sigla_uf"):
    save_path_uf = OUTPUT / "localizacao" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

# Tempo ensino

df_tempo_ensino_classes_comuns = read_sheet("1.45", skiprows=8)
df_tempo_ensino_classes_exclusisas = read_sheet("1.51", skiprows=8)

RENAMES_TEMPO_ENSINO = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Federal": "integral_federal",
    "Estadual": "integral_estadual",
    "Municipal": "integral_municipal",
    "Privada": "integral_privada",
    "Federal.1": "parcial_federal",
    "Estadual.1": "parcial_estadual",
    "Municipal.1": "parcial_municipal",
    "Privada.1": "parcial_privada",
}

df_tempo_ensino_classes_comuns = (
    df_tempo_ensino_classes_comuns.rename(
        columns=RENAMES_TEMPO_ENSINO, errors="raise"
    )[RENAMES_TEMPO_ENSINO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("parcial") or c.startswith("integral")
            ],
            var_name="tempo_ensino",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_tempo_ensino_classes_exclusisas = (
    df_tempo_ensino_classes_exclusisas.rename(
        columns=RENAMES_TEMPO_ENSINO, errors="raise"
    )[RENAMES_TEMPO_ENSINO.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("parcial") or c.startswith("integral")
            ],
            var_name="tempo_ensino",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_tempo_ensino = pd.concat(
    [df_tempo_ensino_classes_exclusisas, df_tempo_ensino_classes_comuns]
).assign(
    sigla_uf=lambda d: d["uf"]
    .apply(lambda uf: uf.strip())
    .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
    quantidade_matricula=lambda d: d["quantidade_matricula"].astype("Int64"),
    rede=lambda d: d["tempo_ensino"].apply(
        lambda v: v.split("_")[-1].title().strip()
    ),
    tempo_ensino=lambda d: d["tempo_ensino"].apply(
        lambda v: v.split("_")[0].title().strip()
    ),
)[
    [
        "id_municipio",
        "tipo_classe",
        "rede",
        "tempo_ensino",
        "quantidade_matricula",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_tempo_ensino.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "tempo_ensino" / "ano=2024" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

# Sexo raca cor

df_sexo_raca_cor_classes_comuns = read_sheet("1.42", skiprows=8)
df_sexo_raca_cor_classes_exclusisas = read_sheet("1.48", skiprows=8)

RENAMES_SEXO_RACA_COR = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Não Declarada": "feminino_Não declarada",
    "Branca": "feminino_Branca",
    "Preta": "feminino_Preta",
    "Parda": "feminino_Parda",
    "Amarela": "feminino_Amarela",
    "Indígena": "feminino_Indígena",
    "Não Declarada.1": "masculino_Não declarada",
    "Branca.1": "masculino_Branca",
    "Preta.1": "masculino_Preta",
    "Parda.1": "masculino_Parda",
    "Amarela.1": "masculino_Amarela",
    "Indígena.1": "masculino_Indígena",
}

df_sexo_raca_cor_classes_comuns = (
    df_sexo_raca_cor_classes_comuns.rename(
        columns=RENAMES_SEXO_RACA_COR, errors="raise"
    )[RENAMES_SEXO_RACA_COR.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("feminino") or c.startswith("masculino")
            ],
            var_name="sexo_raca_cor",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_sexo_raca_cor_classes_exclusisas = (
    df_sexo_raca_cor_classes_exclusisas.rename(
        columns=RENAMES_SEXO_RACA_COR, errors="raise"
    )[RENAMES_SEXO_RACA_COR.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c.startswith("feminino") or c.startswith("masculino")
            ],
            var_name="sexo_raca_cor",
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_sexo_raca_cor = pd.concat(
    [df_sexo_raca_cor_classes_exclusisas, df_sexo_raca_cor_classes_comuns]
).assign(
    sigla_uf=lambda d: d["uf"]
    .apply(lambda uf: uf.strip())
    .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
    quantidade_matricula=lambda d: d["quantidade_matricula"].astype("Int64"),
    raca_cor=lambda d: d["sexo_raca_cor"].apply(
        lambda v: v.split("_")[-1].title().strip()
    ),
    sexo=lambda d: d["sexo_raca_cor"].apply(
        lambda v: v.split("_")[0].title().strip()
    ),
)[
    [
        "id_municipio",
        "tipo_classe",
        "sexo",
        "raca_cor",
        "quantidade_matricula",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_sexo_raca_cor.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "sexo_raca_cor" / "ano=2024" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )


# Tipo deficiencia

df_tipo_deficiencia_classes_comuns = read_sheet("1.44", skiprows=7)
df_tipo_deficiencia_classes_exclusisas = read_sheet("1.50", skiprows=7)

RENAMES_TIPO_DEFICIENCIA = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Cegueira": "Cegueira",
    "Baixa Visão": "Baixa Visão",
    "Surdez": "Surdez",
    "Deficiência Auditiva": "Deficiência Auditiva",
    "Surdocegueira": "Surdocegueira",
    "Deficiência Física": "Deficiência Física",
    "Deficiência Intelectual": "Deficiência Intelectual",
    "Deficiência Múltipla": "Deficiência Múltipla",
    "Transtorno do Espectro Autista": "Transtorno do Espectro Autista",
    "Altas Habilidades / Superdotação": "Altas Habilidades / Superdotação",
}

df_tipo_deficiencia_classes_comuns = (
    df_tipo_deficiencia_classes_comuns.rename(
        columns=RENAMES_TIPO_DEFICIENCIA, errors="raise"
    )[RENAMES_TIPO_DEFICIENCIA.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c not in ["id_municipio", "uf", "etapa_ensino"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_tipo_deficiencia_classes_exclusisas = (
    df_tipo_deficiencia_classes_exclusisas.rename(
        columns=RENAMES_TIPO_DEFICIENCIA, errors="raise"
    )[RENAMES_TIPO_DEFICIENCIA.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c
                for c in d.columns
                if c not in ["id_municipio", "uf", "etapa_ensino"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

df_tipo_deficiencia = (
    pd.concat(
        [
            df_tipo_deficiencia_classes_exclusisas,
            df_tipo_deficiencia_classes_comuns,
        ]
    )
    .assign(
        sigla_uf=lambda d: d["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
        quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
            "Int64"
        ),
    )
    .rename(columns={"variable": "tipo_deficiencia"})[
        [
            "id_municipio",
            "tipo_classe",
            "tipo_deficiencia",
            "quantidade_matricula",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_tipo_deficiencia.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "tipo_deficiencia" / "ano=2024" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

## Subir tabelas

for dir in OUTPUT.iterdir():
    table_id = dir.name
    tb = bd.Table(
        dataset_id="br_inep_educacao_especial",
        table_id=table_id,
    )
    tb.create(
        path=dir,
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )
