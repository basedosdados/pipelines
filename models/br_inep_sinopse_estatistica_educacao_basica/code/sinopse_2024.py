"""
Esse script atualiza as seguintes tabelas do dataset br_inep_sinopse_estatistica_educacao_basica
para 2024.

Tabelas que esse script limpa:

- tempo_ensino
- localizacao
- etapa_ensino_serie
- faixa_etaria
- sexo_raca_cor
"""

import os
import zipfile
import pandas as pd
import basedosdados as bd
from pathlib import Path
import requests
from typing import Optional

ROOT = Path("models") / "br_inep_sinopse_estatistica_educacao_basica"
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_2024.zip"

r = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True)

with open(INPUT / "2024.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2024.zip") as z:
    z.extractall(INPUT)


def read_sheet(sheet_name: str, skiprows: int = 8) -> pd.DataFrame:
    return pd.read_excel(
        INPUT
        / "sinopse_estatistica_censo_escolar_2024"
        / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx",
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


## Etapa ensino serie

sheets_etapa_ensino_serie = {
    "educacao_infantil": "Educação Infantil 1.5",
    "anos_iniciais": "Anos Iniciais 1.15",  # Ensino Fundamental
    "anos_finais": "Anos Finais 1.20",  # Ensino Fundamental
    "ensino_medio": "Ensino Médio 1.25",
    "ensino_profissional": "Educação Profissional 1.30",
    "eja": "EJA 1.35",
}

dfs_etapa_ensino_serie: dict[str, pd.DataFrame] = {
    name: read_sheet(sheet_name)
    for name, sheet_name in sheets_etapa_ensino_serie.items()
}

RENAMES_ETAPA_ENSINO_SERIE = {
    "educacao_infantil": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "creche_total",
        "Federal": "creche_federal",
        "Estadual": "creche_estadual",
        "Municipal": "creche_municipal",
        "Privada": "creche_privada",
        # "Total.1": "_",
        "Federal.1": "pre_escola_federal",
        "Estadual.1": "pre_escola_estadual",
        "Municipal.1": "pre_escola_municipal",
        "Privada.1": "pre_escola_privada",
    },
    "anos_iniciais": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "1_total",
        "Federal": "1_federal",
        "Estadual": "1_estadual",
        "Municipal": "1_municipal",
        "Privada": "1_privada",
        # "Total.1": "2_total",
        "Federal.1": "2_federal",
        "Estadual.1": "2_estadual",
        "Municipal.1": "2_municipal",
        "Privada.1": "2_privada",
        # "Total.2": "3_total",
        "Federal.2": "3_federal",
        "Estadual.2": "3_estadual",
        "Municipal.2": "3_municipal",
        "Privada.2": "3_privada",
        # "Total.3": "4_total",
        "Federal.3": "4_federal",
        "Estadual.3": "4_estadual",
        "Municipal.3": "4_municipal",
        "Privada.3": "4_privada",
        # "Total.4": "5_total",
        "Federal.4": "5_federal",
        "Estadual.4": "5_estadual",
        "Municipal.4": "5_municipal",
        "Privada.4": "5_privada",
    },
    "anos_finais": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "6_total",
        "Federal": "6_federal",
        "Estadual": "6_estadual",
        "Municipal": "6_municipal",
        "Privada": "6_privada",
        # "Total.1": "7_total",
        "Federal.1": "7_federal",
        "Estadual.1": "7_estadual",
        "Municipal.1": "7_municipal",
        "Privada.1": "7_privada",
        # "Total.2": "8_total",
        "Federal.2": "8_federal",
        "Estadual.2": "8_estadual",
        "Municipal.2": "8_municipal",
        "Privada.2": "8_privada",
        # "Total.3": "9_total",
        "Federal.3": "9_federal",
        "Estadual.3": "9_estadual",
        "Municipal.3": "9_municipal",
        "Privada.3": "9_privada",
    },
    "ensino_medio": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "10_total",
        "Federal": "em_1_federal",
        "Estadual": "em_1_estadual",
        "Municipal": "em_1_municipal",
        "Privada": "em_1_privada",
        # "Total.1": "11_total",
        "Federal.1": "em_2_federal",
        "Estadual.1": "em_2_estadual",
        "Municipal.1": "em_2_municipal",
        "Privada.1": "em_2_privada",
        # "Total.2": "12_total",
        "Federal.2": "em_3_federal",
        "Estadual.2": "em_3_estadual",
        "Municipal.2": "em_3_municipal",
        "Privada.2": "em_3_privada",
        # "Total.3": "13_total",
        "Federal.3": "em_4_federal",
        "Estadual.3": "em_4_estadual",
        "Municipal.3": "em_4_municipal",
        "Privada.3": "em_4_privada",
        # "Total.4": "14_total",
        "Federal.4": "em_ns_federal",
        "Estadual.4": "em_ns_estadual",
        "Municipal.4": "em_ns_municipal",
        "Privada.4": "em_ns_privada",
    },
    "ensino_profissional": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "7_total",
        "Federal": "ep_7_federal",
        "Estadual": "ep_7_estadual",
        "Municipal": "ep_7_municipal",
        "Privada": "ep_7_privada",
        # "Total.1": "8_total",
        "Federal.1": "ep_8_federal",
        "Estadual.1": "ep_8_estadual",
        "Municipal.1": "ep_8_municipal",
        "Privada.1": "ep_8_privada",
        # "Total.2": "9_total",
        "Federal.2": "ep_9_federal",
        "Estadual.2": "ep_9_estadual",
        "Municipal.2": "ep_9_municipal",
        "Privada.2": "ep_9_privada",
        # "Total.3": "10_total",
        "Federal.3": "ep_10_federal",
        "Estadual.3": "ep_10_estadual",
        "Municipal.3": "ep_10_municipal",
        "Privada.3": "ep_10_privada",
        # "Total.4": "11_total",
        "Federal.4": "ep_11_federal",
        "Estadual.4": "ep_11_estadual",
        "Municipal.4": "ep_11_municipal",
        "Privada.4": "ep_11_privada",
    },
    "eja": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "13_total",
        "Federal": "eja_ef_federal",
        "Estadual": "eja_ef_estadual",
        "Municipal": "eja_ef_municipal",
        "Privada": "eja_ef_privada",
        # "Total.1": "14_total",
        "Federal.1": "eja_em_federal",
        "Estadual.1": "eja_em_estadual",
        "Municipal.1": "eja_em_municipal",
        "Privada.1": "eja_em_privada",
    },
}


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_drop = [
        col
        for col in df.columns
        if col.startswith("Unnamed") or col.startswith("Total")
    ]

    return df.drop(columns=cols_drop)


dfs_etapa_ensino_serie = {
    name: drop_unused_columns(
        df.rename(columns=RENAMES_ETAPA_ENSINO_SERIE[name], errors="raise")
    )
    for name, df in dfs_etapa_ensino_serie.items()
}


def wide_to_long_etapa_ensino(
    df: pd.DataFrame, var_name: str, value_name: str
) -> pd.DataFrame:
    df = df.copy().loc[(df["id_municipio"].notna()) & (df["id_municipio"] != " "),]  # type: ignore
    assert df["id_municipio"].unique().size == 5570
    value_vars = [
        col
        for col in df.columns
        if col.endswith("federal")
        or col.endswith("estadual")
        or col.endswith("municipal")
        or col.endswith("privada")
    ]
    return pd.melt(
        df,
        id_vars=["id_municipio", "uf"],
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
    )


def create_etapa_ensino(value: str) -> str:
    if value.startswith("creche"):
        return "Educação Infantil – Creche"
    elif value.startswith("pre_escola"):
        return "Educação Infantil – Pré-Escola"
    elif value.startswith("em"):
        return "Ensino Médio Regular"
    elif value.startswith("ep"):
        _, n, _ = value.split("_")
        if n == "7":
            return "Educação Profissional – Associada ao Ensino Médio"
        elif n == "8":
            return "Educação Profissional - Curso Técnico Concomitante"
        elif n == "9":
            return "Educação Profissional - Curso Técnico Subsequente"
        elif n == "10":
            return "Educação Profissional - Curso FIC Concomitante"
        elif n == "11":
            return "Educação Profissional - Curso FIC Integrado na Modalidade EJA"
        else:
            raise Exception(f"Invalid {n=}, {value=}")
    elif value.startswith("eja"):
        _, eja_etapa, _ = value.split("_")

        if eja_etapa == "ef":
            return "EJA – Ensino Fundamental"
        elif eja_etapa == "em":
            return "EJA – Ensino Médio"
        else:
            raise Exception(f"Invalid {eja_etapa=}, {value=}")

    number, _ = value.split("_")
    number = int(number)

    if number <= 5:
        return "Ensino Fundamental – Anos Iniciais"
    elif number >= 6 and number <= 9:
        return "Ensino Fundamental – Anos Finais"

    raise Exception(f"Invalid {number=}, {value=}")


def etapa_ensino_to_serie(value: str) -> Optional[str]:
    serie = value.split("_")

    if serie[0].isdigit() and int(serie[0]) in range(1, 10):
        return f"{serie[0].strip()}º ano do Ensino Fundamental – Anos Iniciais"
    elif value.startswith("em"):
        em_number = serie[1].strip()

        if em_number == "ns":
            return "Ensino Médio Não Seriado"
        elif em_number.isdigit():
            return f"{em_number}º ano do Ensino Médio Regular"
        else:
            raise Exception(f"Invalid {value=}")
    else:
        return None


bd_dir = bd.read_sql(
    "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

df_etapa_ensino_serie = (
    pd.concat(
        [
            wide_to_long_etapa_ensino(
                df, var_name="etapa_ensino", value_name="quantidade_matricula"
            )
            for _, df in dfs_etapa_ensino_serie.items()
        ]
    )
    .assign(
        created_etapa_ensino=lambda d: d["etapa_ensino"]
        .apply(create_etapa_ensino)
        .astype("string"),
        serie=lambda d: d["etapa_ensino"].apply(etapa_ensino_to_serie),
        rede=lambda d: d["etapa_ensino"].apply(lambda v: v.split("_")[-1].title()),
        sigla_uf=lambda d: d["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}),
        quantidade_matricula=lambda d: d["quantidade_matricula"].astype("Int64"),
    )
    .drop(columns=["etapa_ensino"])
    .rename(
        columns={"created_etapa_ensino": "etapa_ensino"},
        errors="raise",
    )[
        [
            "sigla_uf",
            "id_municipio",
            "rede",
            "etapa_ensino",
            "serie",
            "quantidade_matricula",
        ]
    ]
)


for sigla_uf, df in df_etapa_ensino_serie.groupby("sigla_uf"):
    save_path_uf = OUTPUT / "etapa_ensino_serie" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((save_path_uf / "data.csv"), index=False)


## Faixa etaria

sheets_faixa_etaria = {
    "creche": "1.8",
    "pre_escola": "1.12",
    "anos_iniciais": "1.18",
    "anos_finais": "1.23",
    "ensino_medio": "1.28",
    "ensino_profissional": "1.33",
    "eja": "1.38",
}

RENAMES_FAIXA_ETARIA = {
    "creche": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_3_anos",
        "Unnamed: 6": "4_a_5_anos",
        "Unnamed: 7": "6_anos_ou_mais",
    },
    "pre_escola": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_3_anos",
        "Unnamed: 6": "4_a_5_anos",
        "Unnamed: 7": "6_anos_ou_mais",
    },
    "anos_iniciais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_5_anos",
        "Unnamed: 6": "6_a_10_anos",
        "Unnamed: 7": "11_a_14_anos",
        "Unnamed: 8": "15_a_17_anos",
        "Unnamed: 9": "18_a_19_anos",
        "Unnamed: 10": "20_anos_ou_mais",
    },
    "anos_finais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_10_anos",
        "Unnamed: 6": "11_a_14_anos",
        "Unnamed: 7": "15_a_17_anos",
        "Unnamed: 8": "18_a_19_anos",
        "Unnamed: 9": "20_a_24_anos",
        "Unnamed: 10": "25_anos_ou_mais",
    },
    "ensino_medio": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_14_anos",
        "Unnamed: 6": "15_a_17_anos",
        "Unnamed: 7": "18_a_19_anos",
        "Unnamed: 8": "20_a_24_anos",
        "Unnamed: 9": "25_anos_ou_mais",
    },
    "ensino_profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_14_anos",
        "Unnamed: 6": "15_a_17_anos",
        "Unnamed: 7": "18_a_19_anos",
        "Unnamed: 8": "20_a_24_anos",
        "Unnamed: 9": "25_anos_ou_mais",
    },
    "eja": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "ate_14_anos",
        "Unnamed: 6": "15_a_17_anos",
        "Unnamed: 7": "18_a_19_anos",
        "Unnamed: 8": "20_a_24_anos",
        "Unnamed: 9": "25_a_29_anos",
        "Unnamed: 10": "30_a_34_anos",
        "Unnamed: 11": "35_a_39_anos",
        "Unnamed: 12": "40_anos_ou_mais",
    },
}

dfs_faixa_etaria = {
    name: drop_unused_columns(
        read_sheet(sheet_name).rename(
            columns=RENAMES_FAIXA_ETARIA[name], errors="raise"
        )
    )
    for name, sheet_name in sheets_faixa_etaria.items()
}

df_faixa_etaria = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[(d["id_municipio"].notna()) & (df["id_municipio"] != " "),]
        )
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=["id_municipio", "uf"],
                value_vars=[
                    c for c in d.columns if c.endswith("anos") or c.endswith("mais")
                ],
                var_name="faixa_etaria",
                value_name="quantidade_matricula",
            )
        )
        .assign(etapa=etapa)
        for etapa, df in dfs_faixa_etaria.items()
    ]
)

df_faixa_etaria["etapa_ensino"] = df_faixa_etaria["etapa"].replace(
    {
        "creche": "Educação Infantil – Creche",
        "pre_escola": "Educação Infantil – Pré-Escola",
        "anos_iniciais": "Ensino Fundamental – Anos Iniciais",
        "anos_finais": "Ensino Fundamental – Anos Finais",
        "ensino_medio": "Ensino Médio Regular",
        "ensino_profissional": "Educação Profissional",
        "eja": "Educação de Jovens e Adultos (EJA)",
    }
)

df_faixa_etaria["faixa_etaria"] = df_faixa_etaria["faixa_etaria"].replace(
    {
        "ate_3_anos": "Até 3 anos",
        "4_a_5_anos": "4 a 5 anos",
        "6_anos_ou_mais": "6 anos ou mais",
        "ate_5_anos": "Até 5 anos",
        "6_a_10_anos": "6 a 10 anos",
        "11_a_14_anos": "11 a 14 anos",
        "15_a_17_anos": "15 a 17 anos",
        "18_a_19_anos": "18 a 19 anos",
        "20_anos_ou_mais": "20 anos ou mais",
        "ate_10_anos": "Até 10 anos",
        "20_a_24_anos": "20 a 24 anos",
        "25_anos_ou_mais": "25 anos ou mais",
        "ate_14_anos": "Até 14 anos",
        "25_a_29_anos": "25 a 29 anos",
        "30_a_34_anos": "30 a 34 anos",
        "35_a_39_anos": "35 a 39 anos",
        "40_anos_ou_mais": "40 anos ou mais",
    }
)

df_faixa_etaria["uf"] = (
    df_faixa_etaria["uf"]
    .apply(lambda uf: uf.strip())
    .replace(
        {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
    )
)

df_faixa_etaria = df_faixa_etaria[
    ["uf", "id_municipio", "etapa_ensino", "faixa_etaria", "quantidade_matricula"]
].rename(columns={"uf": "sigla_uf"})  # type: ignore

df_faixa_etaria["quantidade_matricula"] = df_faixa_etaria[
    "quantidade_matricula"
].astype("Int64")

for sigla_uf, df in df_faixa_etaria.groupby("sigla_uf"):
    path = OUTPUT / "faixa_etaria" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)

## Localizacao

sheets_localizacao = {
    "creche": "Creche 1.6",
    "pre_escola": "Pré-Escola 1.10",
    "anos_iniciais": "1.16",
    "anos_finais": "1.21",
    "ensino_medio": "1.26",
    "ensino_profissional": "1.31",
    "eja": "1.36",
    "classes_comuns": "1.41",
    "classes_exclusivas": "1.47",
}

dfs_localizacao = {
    name: drop_unused_columns(
        read_sheet(sheet_name).rename(
            columns={
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
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_localizacao.items()
}

df_localizacao = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[(d["id_municipio"].notna()) & (df["id_municipio"] != " "),]
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
        .assign(etapa=etapa)
        for etapa, df in dfs_localizacao.items()
    ]
)

df_localizacao["uf"] = (
    df_localizacao["uf"]
    .apply(lambda uf: uf.strip())
    .replace(
        {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
    )
)

df_localizacao["rede"] = df_localizacao["localizacao"].apply(
    lambda v: v.split("_")[-1].title()
)

df_localizacao["localizacao"] = df_localizacao["localizacao"].apply(
    lambda v: v.split("_")[0].title()
)

df_localizacao["etapa_ensino"] = df_localizacao["etapa"].replace(
    {
        "creche": "Educação Infantil – Creche",
        "pre_escola": "Educação Infantil – Pré-Escola",
        "anos_iniciais": "Ensino Fundamental – Anos Iniciais",
        "anos_finais": "Ensino Fundamental – Anos Finais",
        "ensino_medio": "Ensino Médio Regular",
        "ensino_profissional": "Educação Profissional",
        "eja": "Educação de Jovens e Adultos (EJA)",
        "classes_comuns": "Educação Especial – Classes Comuns",
        "classes_exclusivas": "Educação Especial – Classes Exclusivas",
    }
)

df_localizacao["quantidade_matricula"] = df_localizacao["quantidade_matricula"].astype(
    "Int64"
)

df_localizacao = df_localizacao.rename(columns={"uf": "sigla_uf"})[
    [
        "sigla_uf",
        "id_municipio",
        "rede",
        "etapa_ensino",
        "localizacao",
        "quantidade_matricula",
    ]
]

for sigla_uf, df in df_localizacao.groupby("sigla_uf"):
    path = OUTPUT / "localizacao" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


# Tempo ensino

sheets_tempo_ensino = {
    "creche": "1.9",
    "pre_escola": "1.13",
    "anos_iniciais": "1.19",
    "anos_finais": "1.24",
    "ensino_medio": "1.29",
    "classes_comuns": "1.45",
    "classes_exclusivas": "1.51",
}

dfs_tempo_ensino = {
    name: drop_unused_columns(
        read_sheet(sheet_name).rename(
            columns={
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
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_tempo_ensino.items()
}

df_tempo_ensino = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[(d["id_municipio"].notna()) & (df["id_municipio"] != " "),]
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
        .assign(etapa=etapa)
        for etapa, df in dfs_tempo_ensino.items()
    ]
)


df_tempo_ensino["uf"] = (
    df_tempo_ensino["uf"]
    .apply(lambda uf: uf.strip())
    .replace(
        {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
    )
)

df_tempo_ensino["rede"] = df_tempo_ensino["tempo_ensino"].apply(
    lambda v: v.split("_")[-1].title()
)

df_tempo_ensino["etapa_ensino"] = df_tempo_ensino["etapa"].replace(
    {
        "creche": "Educação Infantil – Creche",
        "pre_escola": "Educação Infantil – Pré-Escola",
        "anos_iniciais": "Ensino Fundamental – Anos Iniciais",
        "anos_finais": "Ensino Fundamental – Anos Finais",
        "ensino_medio": "Ensino Médio Regular",
        "classes_comuns": "Educação Especial – Classes Comuns",
        "classes_exclusivas": "Educação Especial – Classes Exclusivas",
    }
)

df_tempo_ensino["tempo_ensino"] = df_tempo_ensino["tempo_ensino"].apply(
    lambda v: v.split("_")[0].title()
)

df_tempo_ensino["quantidade_matricula"] = df_tempo_ensino[
    "quantidade_matricula"
].astype("Int64")


df_tempo_ensino = df_tempo_ensino.rename(columns={"uf": "sigla_uf"})[
    [
        "sigla_uf",
        "id_municipio",
        "rede",
        "etapa_ensino",
        "tempo_ensino",
        "quantidade_matricula",
    ]
]

for sigla_uf, df in df_tempo_ensino.groupby("sigla_uf"):
    path = OUTPUT / "tempo_ensino" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


## Sexo raca/cor

sheets_sexo_raca_cor = {
    "creche": "1.7",
    "pre_escola": "1.11",
    "anos_iniciais": "1.17",
    "anos_finais": "1.22",
    "ensino_medio": "1.27",
    "ensino_profissional": "1.32",
    "eja": "1.37",
    "classes_comuns": "1.42",
    "classes_exclusivas": "1.48",
}

dfs_sexo_raca_cor = {
    name: drop_unused_columns(
        read_sheet(sheet_name).rename(
            columns={
                "Unnamed: 1": "uf",
                "Unnamed: 3": "id_municipio",
                "Não Declarada": "feminino_nao-declarada",
                "Branca": "feminino_branca",
                "Preta": "feminino_preta",
                "Parda": "feminino_parda",
                "Amarela": "feminino_amarela",
                "Indígena": "feminino_indigera",
                "Não Declarada.1": "masculino_nao-declarada",
                "Branca.1": "masculino_branca",
                "Preta.1": "masculino_preta",
                "Parda.1": "masculino_parda",
                "Amarela.1": "masculino_amarela",
                "Indígena.1": "masculino_indigera",
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_sexo_raca_cor.items()
}

df_sexo_raca_cor = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[(d["id_municipio"].notna()) & (df["id_municipio"] != " "),]
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
        .assign(etapa=etapa)
        for etapa, df in dfs_sexo_raca_cor.items()
    ]
)

df_sexo_raca_cor["etapa"].unique()

df_sexo_raca_cor["etapa_ensino"] = df_sexo_raca_cor["etapa"].replace(
    {
        "creche": "Educação Infantil – Creche",
        "pre_escola": "Educação Infantil – Pré-Escola",
        "anos_iniciais": "Ensino Fundamental – Anos Iniciais",
        "anos_finais": "Ensino Fundamental – Anos Finais",
        "ensino_medio": "Ensino Médio Regular",
        "ensino_profissional": "Educação Profissional",
        "eja": "Educação de Jovens e Adultos (EJA)",
        "classes_comuns": "Educação Especial – Classes Comuns",
        "classes_exclusivas": "Educação Especial – Classes Exclusivas",
    }
)

df_sexo_raca_cor["uf"] = (
    df_sexo_raca_cor["uf"]
    .apply(lambda uf: uf.strip())
    .replace(
        {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
    )
)


df_sexo_raca_cor["sexo"] = df_sexo_raca_cor["sexo_raca_cor"].apply(
    lambda v: v.split("_")[0].title()
)

df_sexo_raca_cor["raca_cor"] = df_sexo_raca_cor["sexo_raca_cor"].apply(
    lambda v: v.split("_")[-1]
)

df_sexo_raca_cor["raca_cor"].unique()

df_sexo_raca_cor["raca_cor"] = df_sexo_raca_cor["raca_cor"].replace(
    {
        "nao-declarada": "Não declarada",
        "branca": "Branca",
        "preta": "Preta",
        "parda": "Parda",
        "amarela": "Amarela",
        "indigera": "Indígena",
    }
)

df_sexo_raca_cor["raca_cor"].unique()

df_sexo_raca_cor["quantidade_matricula"] = df_sexo_raca_cor[
    "quantidade_matricula"
].astype("Int64")


df_sexo_raca_cor = df_sexo_raca_cor.rename(columns={"uf": "sigla_uf"})[
    [
        "sigla_uf",
        "id_municipio",
        "etapa_ensino",
        "sexo",
        "raca_cor",
        "quantidade_matricula",
    ]
]

for sigla_uf, df in df_sexo_raca_cor.groupby("sigla_uf"):
    path = OUTPUT / "sexo_raca_cor" / "ano=2024" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


## Subir tabelas

for dir in OUTPUT.iterdir():
    table_id = dir.name
    tb = bd.Table(
        dataset_id="br_inep_sinopse_estatistica_educacao_basica", table_id=table_id
    )
    tb.create(
        path=dir,
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )

for table_id in os.listdir(OUTPUT):
    tb = bd.Table(
        dataset_id="br_inep_sinopse_estatistica_educacao_basica", table_id=table_id
    )
    tb.create(
        path=os.path.join(OUTPUT, table_id),
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )
