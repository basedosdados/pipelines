# ruff: noqa: RUF001
"""
Esse script atualiza as seguintes tabelas do dataset br_inep_sinopse_estatistica_educacao_basica
para 2025.

- etapa_ensino_serie
- localizacao
- tempo_ensino
- sexo_raca_cor
- faixa_etaria
- docente_etapa_ensino
- docente_localizacao
- docente_escolaridade
- docente_deficiencia
- docente_faixa_etaria_sexo
- docente_regime_contrato
"""

import os
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

INPUT = Path("input") / "br_inep_sinopse_estatistica_educacao_basica"
OUTPUT = Path("output") / "br_inep_sinopse_estatistica_educacao_basica"

INPUT.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)

URL = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_2025.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "2025.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2025.zip") as z:
    z.extractall(INPUT)


def read_sheet(sheet_name: str, skiprows: int = 8) -> pd.DataFrame:
    return pd.read_excel(
        INPUT
        / "Sinopse_Estatistica_da_Educaç╞o_Basica_2025"
        / "Sinopse_Estatistica_da_Educaç╞o_Basica_2025.xlsx",
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


def rename_df_by_sheet(
    df: pd.DataFrame, sheet_name: str, renames: dict[str, str]
) -> pd.DataFrame:
    try:
        return df.rename(columns=renames, errors="raise")
    except Exception as e:
        raise Exception(f"Failed to rename sheet {sheet_name}") from e


## Etapa ensino serie

sheets_etapa_ensino_serie = {
    "educacao_infantil": ("Educação Infantil 1.8", 11),
    "anos_iniciais": ("Anos Iniciais 1.22", 10),  # Ensino Fundamental
    "anos_finais": ("Anos Finais 1.28", 10),  # Ensino Fundamental
    "ensino_medio": ("1.35", 10),
    "ensino_profissional": ("Educação Profissional 1.42", 12),
    "eja": ("EJA 1.49", 11),
}

dfs_etapa_ensino_serie: dict[str, pd.DataFrame] = {
    name: read_sheet(sheet_name, skiprows=skiprows)
    for name, (sheet_name, skiprows) in sheets_etapa_ensino_serie.items()
}

RENAMES_ETAPA_ENSINO_SERIE = {
    "educacao_infantil": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "creche_total",
        "Unnamed: 16": "creche_federal",
        "Unnamed: 17": "creche_estadual",
        "Unnamed: 18": "creche_municipal",
        "Unnamed: 19": "creche_privada",
        # "Total.1": "_",
        "Unnamed: 26": "pre_escola_federal",
        "Unnamed: 27": "pre_escola_estadual",
        "Unnamed: 28": "pre_escola_municipal",
        "Unnamed: 29": "pre_escola_privada",
    },
    "anos_iniciais": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "1_total",
        "Unnamed: 9": "1_federal",
        "Unnamed: 10": "1_estadual",
        "Unnamed: 11": "1_municipal",
        "Unnamed: 12": "1_privada",
        # "Total.1": "2_total",
        "Unnamed: 17": "2_federal",
        "Unnamed: 18": "2_estadual",
        "Unnamed: 19": "2_municipal",
        "Unnamed: 22": "2_privada",
        # "Total.2": "3_total",
        "Unnamed: 29": "3_federal",
        "Unnamed: 30": "3_estadual",
        "Unnamed: 31": "3_municipal",
        "Unnamed: 32": "3_privada",
        # "Total.3": "4_total",
        "Unnamed: 39": "4_federal",
        "Unnamed: 40": "4_estadual",
        "Unnamed: 41": "4_municipal",
        "Unnamed: 42": "4_privada",
        # "Total.4": "5_total",
        "Unnamed: 49": "5_federal",
        "Unnamed: 50": "5_estadual",
        "Unnamed: 51": "5_municipal",
        "Unnamed: 52": "5_privada",
    },
    "anos_finais": {
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "1_total",
        "Unnamed: 9": "6_federal",
        "Unnamed: 10": "6_estadual",
        "Unnamed: 11": "6_municipal",
        "Unnamed: 12": "6_privada",
        # "Total.1": "2_total",
        "Unnamed: 19": "7_federal",
        "Unnamed: 20": "7_estadual",
        "Unnamed: 21": "7_municipal",
        "Unnamed: 22": "7_privada",
        # "Total.2": "3_total",
        "Unnamed: 29": "8_federal",
        "Unnamed: 30": "8_estadual",
        "Unnamed: 31": "8_municipal",
        "Unnamed: 32": "8_privada",
        # "Total.3": "4_total",
        "Unnamed: 39": "9_federal",
        "Unnamed: 40": "9_estadual",
        "Unnamed: 41": "9_municipal",
        "Unnamed: 42": "9_privada",
        # "Total.4": "5_total",
        # "Unnamed: 49": "5_federal",
        # "Unnamed: 50": "5_estadual",
        # "Unnamed: 51": "5_municipal",
        # "Unnamed: 52": "5_privada"
    },
    "ensino_medio": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "10_total",
        "Unnamed: 9": "em_1_federal",
        "Unnamed: 10": "em_1_estadual",
        "Unnamed: 11": "em_1_municipal",
        "Unnamed: 12": "em_1_privada",
        # "Total.1": "11_total",
        "Unnamed: 19": "em_2_federal",
        "Unnamed: 20": "em_2_estadual",
        "Unnamed: 21": "em_2_municipal",
        "Unnamed: 22": "em_2_privada",
        # "Total.2": "12_total",
        "Unnamed: 29": "em_3_federal",
        "Unnamed: 30": "em_3_estadual",
        "Unnamed: 31": "em_3_municipal",
        "Unnamed: 32": "em_3_privada",
        # "Total.3": "13_total",
        "Unnamed: 39": "em_4_federal",
        "Unnamed: 40": "em_4_estadual",
        "Unnamed: 41": "em_4_municipal",
        "Unnamed: 42": "em_4_privada",
        # "Total.4": "14_total",
        "Unnamed: 49": "em_ns_federal",
        "Unnamed: 50": "em_ns_estadual",
        "Unnamed: 51": "em_ns_municipal",
        "Unnamed: 52": "em_ns_privada",
    },
    "ensino_profissional": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "7_total",
        "Unnamed: 19": "ep_1_federal",
        "Unnamed: 20": "ep_1_estadual",
        "Unnamed: 21": "ep_1_municipal",
        "Unnamed: 22": "ep_1_municipal",
        "Unnamed: 29": "ep_7_federal",
        "Unnamed: 30": "ep_7_estadual",
        "Unnamed: 31": "ep_7_municipal",
        "Unnamed: 32": "ep_7_privada",
        "Unnamed: 39": "ep_8_federal",
        "Unnamed: 40": "ep_8_estadual",
        "Unnamed: 41": "ep_8_municipal",
        "Unnamed: 42": "ep_8_privada",
        "Unnamed: 49": "ep_9_federal",
        "Unnamed: 50": "ep_9_estadual",
        "Unnamed: 51": "ep_9_municipal",
        "Unnamed: 52": "ep_9_privada",
        "Unnamed: 59": "ep_12_federal",
        "Unnamed: 60": "ep_12_estadual",
        "Unnamed: 61": "ep_12_municipal",
        "Unnamed: 62": "ep_12_privada",
        "Unnamed: 69": "ep_13_federal",
        "Unnamed: 70": "ep_13_estadual",
        "Unnamed: 71": "ep_13_municipal",
        "Unnamed: 72": "ep_13_privada",
        # "Total.3": "10_total",
        "Unnamed: 80": "ep_10_federal",
        "Unnamed: 81": "ep_10_estadual",
        "Unnamed: 82": "ep_10_municipal",
        "Unnamed: 83": "ep_10_privada",
        "Unnamed: 90": "ep_14_federal",
        "Unnamed: 91": "ep_14_estadual",
        "Unnamed: 92": "ep_14_municipal",
        "Unnamed: 93": "ep_14_privada",
        "Unnamed: 100": "ep_15_federal",
        "Unnamed: 101": "ep_15_estadual",
        "Unnamed: 102": "ep_15_municipal",
        "Unnamed: 103": "ep_15_privada",
        "Unnamed: 110": "ep_16_federal",
        "Unnamed: 111": "ep_16_estadual",
        "Unnamed: 112": "ep_16_municipal",
        "Unnamed: 113": "ep_16_privada",
        "Unnamed: 120": "ep_17_federal",
        "Unnamed: 121": "ep_17_estadual",
        "Unnamed: 122": "ep_17_municipal",
        "Unnamed: 123": "ep_17_privada",
        # "Federal.4": "ep_11_federal",
        # "Estadual.4": "ep_11_estadual",
        # "Municipal.4": "ep_11_municipal",
        # "Privada.4": "ep_11_privada",
    },
    "eja": {
        # "Unnamed: 0": "regiao_geografica",
        "Unnamed: 1": "uf",
        # "Unnamed: 2": "municipio",
        "Unnamed: 3": "id_municipio",
        # "Total": "13_total",
        "Unnamed: 9": "eja_ef_federal",
        "Unnamed: 10": "eja_ef_estadual",
        "Unnamed: 11": "eja_ef_municipal",
        "Unnamed: 12": "eja_ef_privada",
        # "Total.1": "14_total",
        "Unnamed: 19": "eja_em_federal",
        "Unnamed: 20": "eja_em_estadual",
        "Unnamed: 21": "eja_em_municipal",
        "Unnamed: 22": "eja_em_privada",
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
        # df.rename(columns=RENAMES_ETAPA_ENSINO_SERIE[name], errors="raise")
        rename_df_by_sheet(df, name, RENAMES_ETAPA_ENSINO_SERIE[name])
    )
    for name, df in dfs_etapa_ensino_serie.items()
}


def wide_to_long_etapa_ensino(
    df: pd.DataFrame, var_name: str, value_name: str
) -> pd.DataFrame:
    df: pd.DataFrame = df.copy().loc[  # type: ignore
        (df["id_municipio"].notna()) & (df["id_municipio"].str.strip() != ""),
    ]  # type: ignore
    # assert df["id_municipio"].unique().size == 5570, "id_municipio count != 5570"
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
        if n == "1":
            return "Educação Profissional – Ensino Médio articulado ao Itinerário Formativo Técnico Profissional – Curso Técnico"
        # elif n == "2":
        #     return "Educação Profissional - Ensino Médio Normal/Magistério"
        elif n == "7":
            return "Educação Profissional – Associada ao Ensino Médio"
        elif n == "8":
            # TODO add big dash
            return "Educação Profissional – Curso Técnico Concomitante"
        elif n == "9":
            return "Educação Profissional – Curso Técnico Subsequente"
        elif n == "10":
            return "Educação Profissional – Curso FIC Concomitante"
        elif n == "11":
            return (
                "Educação Profissional – Curso FIC Integrado na Modalidade EJA"
            )
        elif n == "12":
            return "Educação Profissional – Itinerário Formativo Técnico Profissional Exclusivo – Curso Técnico (não articulado ao ensino médio regular)"
        elif n == "13":
            return "Educação Profissional – Curso Técnico Integrado à EJA de Nível Médio"
        elif n == "14":
            return "Educação Profissional – Curso FIC Integrado na Modalidade EJA de Nível Fundamental"
        elif n == "15":
            return "Educação Profissional – Curso FIC Integrado na Modalidade EJA de Nível Médio"
        elif n == "16":
            return "Educação Profissional – Ensino Médio articulado ao Itinerário Formativo Técnico Profissional – Qualificação Profissional"
        elif n == "17":
            return "Educação Profissional – Itinerário Formativo Técnico Profissional Exclusivo – Qualificação Profissional (não articulado ao ensino médio regular)"
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


def etapa_ensino_to_serie(value: str) -> str | None:
    serie = value.split("_")

    if serie[0].isdigit() and int(serie[0]) in range(1, 10):
        ano_etapa = "Iniciais" if int(serie[0]) <= 5 else "Finais"
        return (
            f"{serie[0].strip()}º ano do Ensino Fundamental – Anos {ano_etapa}"
        )
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
        created_etapa_ensino=lambda d: (
            d["etapa_ensino"].apply(create_etapa_ensino).astype("string")
        ),
        serie=lambda d: d["etapa_ensino"].apply(etapa_ensino_to_serie),
        rede=lambda d: d["etapa_ensino"].apply(
            lambda v: v.split("_")[-1].title()
        ),
        sigla_uf=lambda d: (
            d["uf"]
            .apply(lambda uf: uf.strip())
            .replace(
                {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}
            )
        ),
        quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
            "Int64"
        ),
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

df_etapa_ensino_serie["rede"].unique()
df_etapa_ensino_serie["etapa_ensino"].unique()
df_etapa_ensino_serie["serie"].unique()
df_etapa_ensino_serie[["etapa_ensino", "serie"]].value_counts(
    dropna=False
).reset_index()


for sigla_uf, df in df_etapa_ensino_serie.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "etapa_ensino_serie" / "ano=2025" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )


## Faixa etaria

sheets_faixa_etaria = {
    "creche": "1.13",
    "pre_escola": "1.18",
    "anos_iniciais": "1.26",
    "anos_finais": "1.32",
    "ensino_medio": "1.40",
    "ensino_profissional": "1.46",
    "eja": "1.53",
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


df_faixa_etaria: pd.DataFrame = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
            ]
        )
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=["id_municipio", "uf"],
                value_vars=[
                    c
                    for c in d.columns
                    if c.endswith("anos") or c.endswith("mais")
                ],
                var_name="faixa_etaria",
                value_name="quantidade_matricula",
            )
        )
        .assign(etapa=etapa)
        for etapa, df in dfs_faixa_etaria.items()
    ]
)

df_faixa_etaria["etapa"].unique()

df_faixa_etaria["etapa_ensino"] = df_faixa_etaria["etapa"].replace(
    {
        "creche": "Educação Infantil - Creche",
        "pre_escola": "Educação Infantil - Pré-Escola",
        "anos_iniciais": "Ensino Fundamental - Anos Iniciais",
        "anos_finais": "Ensino Fundamental - Anos Finais",
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
    [
        "uf",
        "id_municipio",
        "etapa_ensino",
        "faixa_etaria",
        "quantidade_matricula",
    ]
].rename(columns={"uf": "sigla_uf"})  # type: ignore

df_faixa_etaria["quantidade_matricula"] = df_faixa_etaria[
    "quantidade_matricula"
].astype("Int64")

df_faixa_etaria["etapa_ensino"].unique()
df_faixa_etaria["faixa_etaria"].unique()

for sigla_uf, df in df_faixa_etaria.groupby("sigla_uf"):
    path = OUTPUT / "faixa_etaria" / "ano=2025" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


## Localizacao

sheets_localizacao = {
    "creche": "Creche 1.10",
    "pre_escola": "Pré-Escola 1.15",
    "anos_iniciais": "1.23",
    "anos_finais": "1.29",
    "ensino_medio": "1.37",
    "ensino_profissional": "1.43",
    "eja": "1.50",
    "classes_comuns": "1.61",
    "classes_exclusivas": "1.68",
}

dfs_localizacao = {
    name: drop_unused_columns(
        read_sheet(sheet_name, skiprows=10).rename(
            columns={
                "Unnamed: 1": "uf",
                "Unnamed: 3": "id_municipio",
                "Unnamed: 18": "urbana_federal",
                "Unnamed: 19": "urbana_estadual",
                "Unnamed: 20": "urbana_municipal",
                "Unnamed: 21": "urbana_privada",
                "Unnamed: 28": "rural_federal",
                "Unnamed: 29": "rural_estadual",
                "Unnamed: 30": "rural_municipal",
                "Unnamed: 31": "rural_privada",
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_localizacao.items()
}

df_localizacao = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
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

df_localizacao["quantidade_matricula"] = df_localizacao[
    "quantidade_matricula"
].astype("Int64")

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
    path = OUTPUT / "localizacao" / "ano=2025" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


# Tempo ensino

sheets_tempo_ensino = {
    "creche": "1.14",
    "pre_escola": "1.19",
    "anos_iniciais": "1.19",
    "anos_finais": "1.33",
    "ensino_medio": "1.41",
    "classes_comuns": "1.66",
    "classes_exclusivas": "1.73",
}

dfs_tempo_ensino = {
    name: drop_unused_columns(
        read_sheet(sheet_name, skiprows=11).rename(
            columns={
                "Unnamed: 1": "uf",
                "Unnamed: 3": "id_municipio",
                "Unnamed: 9": "integral_federal",
                "Unnamed: 10": "integral_estadual",
                "Unnamed: 11": "integral_municipal",
                "Unnamed: 12": "integral_privada",
                "Unnamed: 19": "parcial_federal",
                "Unnamed: 20": "parcial_estadual",
                "Unnamed: 21": "parcial_municipal",
                "Unnamed: 22": "parcial_privada",
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_tempo_ensino.items()
}

df_tempo_ensino = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
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
    path = OUTPUT / "tempo_ensino" / "ano=2025" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


## Sexo raca/cor

renames_sexo_raca_cor = {
    "Unnamed: 1": "uf",
    "Unnamed: 3": "id_municipio",
    "Unnamed: 6": "feminino_nao-declarada",
    "Unnamed: 7": "feminino_branca",
    "Unnamed: 8": "feminino_preta",
    "Unnamed: 9": "feminino_parda",
    "Unnamed: 10": "feminino_amarela",
    "Unnamed: 11": "feminino_indigena",
    "Unnamed: 13": "masculino_nao-declarada",
    "Unnamed: 14": "masculino_branca",
    "Unnamed: 15": "masculino_preta",
    "Unnamed: 16": "masculino_parda",
    "Unnamed: 17": "masculino_amarela",
    "Unnamed: 18": "masculino_indigena",
}

sheets_sexo_raca_cor = {
    "creche": "1.12",
    "pre_escola": "1.17",
    "anos_iniciais": "1.25",
    "anos_finais": "1.31",
    "ensino_medio": "1.39",
    "ensino_profissional": "1.45",
    "eja": "1.52",
    "classes_comuns": "1.63",
    "classes_exclusivas": "1.70",
}

dfs_sexo_raca_cor = {
    name: drop_unused_columns(
        rename_df_by_sheet(
            read_sheet(sheet_name, skiprows=9),
            sheet_name=sheet_name,
            renames=renames_sexo_raca_cor,
        )
    )
    for name, sheet_name in sheets_sexo_raca_cor.items()
}

df_sexo_raca_cor = pd.concat(
    [
        df.pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
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
        "indigena": "Indígena",
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
    path = OUTPUT / "sexo_raca_cor" / "ano=2025" / f"sigla_uf={sigla_uf}"
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv((path / "data.csv"), index=False)


## Docente etapa ensino

sheets_docente_etapa_ensino = {
    "Educacao Basica": ("Educação Básica 2.1", 9),
    "Educacao Infantil": ("Educação Infantil 2.7", 11),
    "Ensino Fundamental": ("Ensino Fundamental 2.20", 11),
    "Educacao Profissional": ("Educação Profissional 2.43", 12),
    "EJA": ("EJA 2.49", 11),
    "Educacao Especial": ("Educação Especial 2.55", 11),
    "Educacao Especial - Classes Comuns": ("Classes Comuns 2.62", 11),
    "Educacao Especial - Classes Exclusivas": ("Classes Exclusivas 2.69", 11),
    "Educacao Indigena": ("Educação Indígena 2.77", 11),
}

RENAMES_DOCENTE_ETAPA_ENSINO = {
    "Educacao Basica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola9": "Educação Infantil - Pré Escola",
        # "Total12": "",
        "Anos Iniciais11": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais12": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico ": "Ensino Médio - Propedêutico",
        "Ensino Médio Normal/Magistério": "Ensino Médio - Normal/Magistério",
        "Articulado ao Itinerário Formativo Técnico Profissional (IFTP)": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        "Unnamed: 15": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        " Itinerário Formativo Exclusivo": "Ensino Médio - Itinerário Formativo Exclusivo",
        "Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional  - Curso Técnico": "Educação Profissional - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        "Ensino Médio Normal/Magistério.1": "Educação Profissional - Ensino Médio Normal/Magistério",
        "Curso Técnico Concomitante": "Educação Profissional - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional - Curso Técnico Subsequente",
        "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico\n(não articulado ao ensino médio regular)": "Educação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso Técnico Integrado à EJA de Nível Médio": "Educação Profissional - Curso Técnico Integrado à EJA de Nível Médio",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA17": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional": "Educação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional \n(não articulado ao ensino médio regular)": "Educação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        "Ensino Fundamental19": "EJA - Ensino Fundamental",
        "Ensino Médio20": "EJA - Ensino Médio",
        "Classes Comuns22": "Educação Especial - Classes Comuns",
        "Classes Exclusivas23": "Educação Especial - Classes Exclusivas",
        # "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        # "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        # "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        # "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        # "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        # "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Ensino Fundamental22": "EJA - Ensino Fundamental",
        # "Ensino Médio23": "EJA - Ensino Médio",
        # "Classes Comuns25": "Educação Especial - Classes Comuns",
        # "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
    },
    "Educacao Infantil": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 15": "Creche - Pública",
        "Unnamed: 16": "Creche - Federal",
        "Unnamed: 17": "Creche - Estadual",
        "Unnamed: 18": "Creche - Municipal",
        "Unnamed: 19": "Creche - Privada",
        "Unnamed: 25": "Pré-Escola - Pública",
        "Unnamed: 26": "Pré-Escola - Federal",
        "Unnamed: 27": "Pré-Escola - Estadual",
        "Unnamed: 28": "Pré-Escola - Municipal",
        "Unnamed: 29": "Pré-Escola - Privada",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 15": "Anos Iniciais - Pública",
        "Unnamed: 16": "Anos Iniciais - Federal",
        "Unnamed: 17": "Anos Iniciais - Estadual",
        "Unnamed: 18": "Anos Iniciais - Municipal",
        "Unnamed: 19": "Anos Iniciais - Privada",
        "Unnamed: 25": "Anos Finais - Pública",
        "Unnamed: 26": "Anos Finais - Federal",
        "Unnamed: 27": "Anos Finais - Estadual",
        "Unnamed: 28": "Anos Finais - Municipal",
        "Unnamed: 29": "Anos Finais - Privada",
        "Unnamed: 35": "Turmas Multi - Pública",
        "Unnamed: 36": "Turmas Multi - Federal",
        "Unnamed: 37": "Turmas Multi - Estadual",
        "Unnamed: 38": "Turmas Multi - Municipal",
        "Unnamed: 39": "Turmas Multi - Privada",
    },
    "Educacao Profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 16": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico - Pública",
        "Unnamed: 17": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico - Federal",
        "Unnamed: 18": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico - Estadual",
        "Unnamed: 19": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico - Municipal",
        "Unnamed: 20": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico - Privada",
        "Unnamed: 26": "Ensino Médio Normal/Magistério - Pública",
        "Unnamed: 27": "Ensino Médio Normal/Magistério - Federal",
        "Unnamed: 28": "Ensino Médio Normal/Magistério - Estadual",
        "Unnamed: 29": "Ensino Médio Normal/Magistério - Municipal",
        "Unnamed: 30": "Ensino Médio Normal/Magistério - Privada",
        "Unnamed: 36": "Curso Técnico Concomitante - Pública",
        "Unnamed: 37": "Curso Técnico Concomitante - Federal",
        "Unnamed: 38": "Curso Técnico Concomitante - Estadual",
        "Unnamed: 39": "Curso Técnico Concomitante - Municipal",
        "Unnamed: 40": "Curso Técnico Concomitante - Privada",
        "Unnamed: 46": "Curso Técnico Subsequente - Pública",
        "Unnamed: 47": "Curso Técnico Subsequente - Federal",
        "Unnamed: 48": "Curso Técnico Subsequente - Estadual",
        "Unnamed: 49": "Curso Técnico Subsequente - Municipal",
        "Unnamed: 50": "Curso Técnico Subsequente - Privada",
        "Unnamed: 56": "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular) - Pública",
        "Unnamed: 57": "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular) - Federal",
        "Unnamed: 58": "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular) - Estadual",
        "Unnamed: 59": "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular) - Municipal",
        "Unnamed: 60": "Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular) - Privada",
        "Unnamed: 76": "Curso Técnico Integrado à EJA de Nível Médio - Pública",
        "Unnamed: 77": "Curso Técnico Integrado à EJA de Nível Médio - Federal",
        "Unnamed: 78": "Curso Técnico Integrado à EJA de Nível Médio - Estadual",
        "Unnamed: 79": "Curso Técnico Integrado à EJA de Nível Médio - Municipal",
        "Unnamed: 80": "Curso Técnico Integrado à EJA de Nível Médio - Privada",
        "Unnamed: 87": "Curso FIC Concomitante - Pública",
        "Unnamed: 88": "Curso FIC Concomitante - Federal",
        "Unnamed: 89": "Curso FIC Concomitante - Estadual",
        "Unnamed: 90": "Curso FIC Concomitante - Municipal",
        "Unnamed: 91": "Curso FIC Concomitante - Privada",
        "Unnamed: 97": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Pública",
        "Unnamed: 98": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Federal",
        "Unnamed: 99": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Estadual",
        "Unnamed: 100": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Municipal",
        "Unnamed: 101": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Privada",
        "Unnamed: 107": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Pública",
        "Unnamed: 108": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Federal",
        "Unnamed: 109": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Estadual",
        "Unnamed: 110": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Municipal",
        "Unnamed: 111": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Privada",
        "Unnamed: 117": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional - Pública",
        "Unnamed: 118": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional - Federal",
        "Unnamed: 119": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional - Estadual",
        "Unnamed: 120": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional - Municipal",
        "Unnamed: 121": "Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional - Privada",
        "Unnamed: 127": "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular) - Pública",
        "Unnamed: 128": "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular) - Federal",
        "Unnamed: 129": "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular) - Estadual",
        "Unnamed: 130": "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular) - Municipal",
        "Unnamed: 131": "Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular) - Privada",
        ##
        # "Pública": "Curso Técnico Integrado (Ensino Médio Integrado) - Pública",
        # "Federal": "Curso Técnico Integrado (Ensino Médio Integrado) - Federal",
        # "Estadual": "Curso Técnico Integrado (Ensino Médio Integrado) - Estadual",
        # "Municipal": "Curso Técnico Integrado (Ensino Médio Integrado) - Municipal",
        # "Privada": "Curso Técnico Integrado (Ensino Médio Integrado) - Privada",
        # "Pública.1": "Ensino Médio Normal/Magistério - Pública",
        # "Federal.1": "Ensino Médio Normal/Magistério - Federal",
        # "Estadual.1": "Ensino Médio Normal/Magistério - Estadual",
        # "Municipal.1": "Ensino Médio Normal/Magistério - Municipal",
        # "Privada.1": "Ensino Médio Normal/Magistério - Privada",
        # "Pública.2": "Curso Técnico Concomitante - Pública",
        # "Federal.2": "Curso Técnico Concomitante - Federal",
        # "Estadual.2": "Curso Técnico Concomitante - Estadual",
        # "Municipal.2": "Curso Técnico Concomitante - Municipal",
        # "Privada.2": "Curso Técnico Concomitante - Privada",
        # "Pública.3": "Curso Técnico Subsequente - Pública",
        # "Federal.3": "Curso Técnico Subsequente - Federal",
        # "Estadual.3": "Curso Técnico Subsequente - Estadual",
        # "Municipal.3": "Curso Técnico Subsequente - Municipal",
        # "Privada.3": "Curso Técnico Subsequente - Privada",
        # "Pública.4": "Curso Técnico Misto (Concomitante e Subsequente) - Pública",
        # "Federal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Federal",
        # "Estadual.4": "Curso Técnico Misto (Concomitante e Subsequente) - Estadual",
        # "Municipal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Municipal",
        # "Privada.4": "Curso Técnico Misto (Concomitante e Subsequente) - Privada",
        # "Pública.5": "Curso Técnico Integrado a EJA - Pública",
        # "Federal.5": "Curso Técnico Integrado a EJA - Federal",
        # "Estadual.5": "Curso Técnico Integrado a EJA - Estadual",
        # "Municipal.5": "Curso Técnico Integrado a EJA - Municipal",
        # "Privada.5": "Curso Técnico Integrado a EJA - Privada",
        # "Pública.6": "Curso FIC Concomitante - Pública",
        # "Federal.6": "Curso FIC Concomitante - Federal",
        # "Estadual.6": "Curso FIC Concomitante - Estadual",
        # "Municipal.6": "Curso FIC Concomitante - Municipal",
        # "Privada.6": "Curso FIC Concomitante - Privada",
        # "Pública.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Pública",
        # "Federal.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Federal",
        # "Estadual.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Estadual",
        # "Municipal.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Municipal",
        # "Privada.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Privada",
        # "Pública.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Pública",
        # "Federal.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Federal",
        # "Estadual.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Estadual",
        # "Municipal.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Municipal",
        # "Privada.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Privada",
    },
    "EJA": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 6": "Ensino Fundamental - Pública",
        "Unnamed: 7": "Ensino Fundamental - Federal",
        "Unnamed: 8": "Ensino Fundamental - Estadual",
        "Unnamed: 9": "Ensino Fundamental - Municipal",
        "Unnamed: 10": "Ensino Fundamental - Privada",
        "Unnamed: 16": "Ensino Médio - Pública",
        "Unnamed: 17": "Ensino Médio - Federal",
        "Unnamed: 18": "Ensino Médio - Estadual",
        "Unnamed: 19": "Ensino Médio - Municipal",
        "Unnamed: 20": "Ensino Médio - Privada",
    },
    "Educacao Especial": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 8": "Educação Infantil - Creche",
        "Unnamed: 9": "Educação Infantil - Pré Escola",
        "Unnamed: 11": "Ensino Fundamental - Anos Iniciais",
        "Unnamed: 12": "Ensino Fundamental - Anos Finais",
        "Unnamed: 14": "Ensino Médio - Propedêutico",
        "Unnamed: 15": "Ensino Médio - Normal/Magistério",
        "Unnamed: 16": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        "Unnamed: 17": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        "Unnamed: 18": "Ensino Médio - Itinerário Formativo Exclusivo - Turma de Itinerário Formativo de Aprofundamento (IFA) ou Itinerário Formativo Técnico Profissional (IFTP), que possui algum aluno do Ensino Médio Regular",
        "Unnamed: 21": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        "Unnamed: 22": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Normal/Magistério",
        "Unnamed: 23": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Concomitante",
        "Unnamed: 24": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Subsequente",
        "Unnamed: 25": "Educação Profissional - Educação Profissional Técnica - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        "Unnamed: 26": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Misto (Concomitante e Subsequente)",
        "Unnamed: 27": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Integrado à EJA de Nível Médio",
        "Unnamed: 29": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Concomitante",
        "Unnamed: 30": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Unnamed: 31": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        "Unnamed: 32": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        "Unnamed: 34": "EJA - Ensino Fundamental",
        "Unnamed: 35": "EJA - Ensino Médio",
        # "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        # "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        # "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        # "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        # "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Ensino Fundamental22": "EJA - Ensino Fundamental",
        # "Ensino Médio23": "EJA - Ensino Médio",
        # "Classes Comuns25": "Educação Especial - Classes Comuns",
        # "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
    },
    "Educacao Especial - Classes Comuns": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 6": "Educação Infantil - Creche",
        "Unnamed: 7": "Educação Infantil - Pré Escola",
        "Unnamed: 9": "Ensino Fundamental - Anos Iniciais",
        "Unnamed: 10": "Ensino Fundamental - Anos Finais",
        "Unnamed: 12": "Ensino Médio - Propedêutico",
        "Unnamed: 13": "Ensino Médio - Normal/Magistério",
        "Unnamed: 14": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        "Unnamed: 15": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        "Unnamed: 16": "Ensino Médio - Itinerário Formativo Exclusivo - Turma de Itinerário Formativo de Aprofundamento (IFA) ou Itinerário Formativo Técnico Profissional (IFTP), que possui algum aluno do Ensino Médio Regular",
        "Unnamed: 19": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        "Unnamed: 20": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Normal/Magistério",
        "Unnamed: 21": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Concomitante",
        "Unnamed: 22": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Subsequente",
        "Unnamed: 23": "Educação Profissional - Educação Profissional Técnica - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        "Unnamed: 24": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Misto (Concomitante e Subsequente)",
        "Unnamed: 25": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Integrado à EJA de Nível Médio",
        "Unnamed: 27": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Concomitante",
        "Unnamed: 28": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Unnamed: 29": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        "Unnamed: 30": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        "Unnamed: 32": "EJA - Ensino Fundamental",
        "Unnamed: 33": "EJA - Ensino Médio",
        # old
        # "Unnamed: 14": "Ensino Médio - Propedêutico",
        # "Unnamed: 15": "Ensino Médio - Normal/Magistério",
        # "Unnamed: 16": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        # "Unnamed: 17": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        # "Unnamed: 18": "Ensino Médio - Itinerário Formativo Exclusivo - Turma de Itinerário Formativo de Aprofundamento (IFA) ou  Itinerário Formativo Técnico Profissional (IFTP), que possui algum aluno do Ensino Médio Regular",
        # "Unnamed: 21": "Educação Profissional - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        # "Unnamed: 22": "Educação Profissional - Ensino Médio Normal/Magistério",
        # "Unnamed: 23": "Educação Profissional - Curso Técnico Concomitante",
        # "Unnamed: 24": "Educação Profissional - Curso Técnico Subsequente",
        # "Unnamed: 25": "Educação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        # "Unnamed: 26": "Educação Profissional - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Unnamed: 27": "Educação Profissional - Curso Técnico Integrado à EJA de Nível Médio",
        # "Unnamed: 29": "Educação Profissional - Curso FIC Concomitante",
        # "Unnamed: 30": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Unnamed: 31": "Educação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        # "Unnamed: 32": "Educação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        # "Unnamed: 34": "EJA - Ensino Fundamental",
        # "Unnamed: 35": "EJA - Ensino Médio",
        # "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        # "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        # "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        # "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        # "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Ensino Fundamental22": "EJA - Ensino Fundamental",
        # "Ensino Médio23": "EJA - Ensino Médio",
    },
    "Educacao Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 6": "Educação Infantil - Creche",
        "Unnamed: 7": "Educação Infantil - Pré Escola",
        "Unnamed: 9": "Ensino Fundamental - Anos Iniciais",
        "Unnamed: 10": "Ensino Fundamental - Anos Finais",
        "Unnamed: 12": "Ensino Médio - Propedêutico",
        "Unnamed: 13": "Ensino Médio - Normal/Magistério",
        "Unnamed: 14": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        "Unnamed: 15": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        "Unnamed: 16": "Ensino Médio - Itinerário Formativo Exclusivo - Turma de Itinerário Formativo de Aprofundamento (IFA) ou Itinerário Formativo Técnico Profissional (IFTP), que possui algum aluno do Ensino Médio Regular",
        "Unnamed: 19": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        "Unnamed: 20": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Normal/Magistério",
        "Unnamed: 21": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Concomitante",
        "Unnamed: 22": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Subsequente",
        "Unnamed: 23": "Educação Profissional - Educação Profissional Técnica - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        "Unnamed: 24": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Misto (Concomitante e Subsequente)",
        "Unnamed: 25": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Integrado à EJA de Nível Médio",
        "Unnamed: 27": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Concomitante",
        "Unnamed: 28": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Unnamed: 29": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        "Unnamed: 30": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        "Unnamed: 32": "EJA - Ensino Fundamental",
        "Unnamed: 33": "EJA - Ensino Médio",
        # old
        # "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        # "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        # "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        # "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        # "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Ensino Fundamental22": "EJA - Ensino Fundamental",
        # "Ensino Médio23": "EJA - Ensino Médio",
    },
    "Educacao Indigena": {
        "  ": "uf",
        " ": "id_municipio",
        "Unnamed: 6": "Educação Infantil - Creche",
        "Unnamed: 7": "Educação Infantil - Pré Escola",
        "Unnamed: 9": "Ensino Fundamental - Anos Iniciais",
        "Unnamed: 10": "Ensino Fundamental - Anos Finais",
        "Unnamed: 12": "Ensino Médio - Propedêutico",
        "Unnamed: 13": "Ensino Médio - Normal/Magistério",
        "Unnamed: 14": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Curso Técnico",
        "Unnamed: 15": "Ensino Médio - Articulado ao Itinerário Formativo Técnico Profissional (IFTP) - Qualificação Profissional",
        "Unnamed: 16": "Ensino Médio - Itinerário Formativo Exclusivo - Turma de Itinerário Formativo de Aprofundamento (IFA) ou Itinerário Formativo Técnico Profissional (IFTP), que possui algum aluno do Ensino Médio Regular",
        "Unnamed: 19": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Articulado ao Itinerário Formativo Técnico Profissional - Curso Técnico",
        "Unnamed: 20": "Educação Profissional - Educação Profissional Técnica - Ensino Médio Normal/Magistério",
        "Unnamed: 21": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Concomitante",
        "Unnamed: 22": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Subsequente",
        "Unnamed: 23": "Educação Profissional - Educação Profissional Técnica - Itinerário Formativo Técnico Profissional Exclusivo - Curso Técnico (não articulado ao ensino médio regular)",
        "Unnamed: 24": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Misto (Concomitante e Subsequente)",
        "Unnamed: 25": "Educação Profissional - Educação Profissional Técnica - Curso Técnico Integrado à EJA de Nível Médio",
        "Unnamed: 27": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Concomitante",
        "Unnamed: 28": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Unnamed: 29": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Ensino Médio articulado ao Itinerário Formativo Técnico Profissional - Qualificação Profissional",
        "Unnamed: 30": "Educação Profissional - Formação Inicial Continuada (FIC) ou Qualificação Profissional - Itinerário Formativo Técnico Profissional Exclusivo - Qualificação Profissional (não articulado ao ensino médio regular)",
        "Unnamed: 32": "EJA - Ensino Fundamental",
        "Unnamed: 33": "EJA - Ensino Médio",
        "Unnamed: 35": "Educação Especial - Classes Comuns",
        "Unnamed: 36": "Educação Especial - Classes Exclusivas",
        # old
        # "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        # "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        # "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        # "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        # "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        # "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        # "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        # "Ensino Fundamental22": "EJA - Ensino Fundamental",
        # "Ensino Médio23": "EJA - Ensino Médio",
        # "Classes Comuns25": "Educação Especial - Classes Comuns",
        # "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
    },
}

dfs_docente_etapa_ensino: dict[str, pd.DataFrame] = {
    name: drop_unused_columns(
        rename_df_by_sheet(
            read_sheet(sheet_name, skiprows=skiprows),
            sheet_name=sheet_name,
            renames=RENAMES_DOCENTE_ETAPA_ENSINO[name],
        )
    )[RENAMES_DOCENTE_ETAPA_ENSINO[name].values()]
    for name, (sheet_name, skiprows) in sheets_docente_etapa_ensino.items()
}

# dfs_docente_etapa_ensino = {
#     name: drop_unused_columns(
#         df.rename(columns=RENAMES_DOCENTE_ETAPA_ENSINO[name], errors="raise")
#     )
#     for name, df in dfs_docente_etapa_ensino.items()
# }

df_docente_etapa_ensino = (
    pd.concat(
        [
            df.assign(tipo_classe=tipo_classe)
            .pipe(
                lambda d: d.loc[
                    (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
                ]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf", "tipo_classe"],
                    value_vars=[
                        c
                        for c in d.columns
                        if c
                        not in ["id_municipio", "uf", "tipo_classe", "rede"]
                    ],
                    value_name="quantidade_docentes",
                )
            )
            for tipo_classe, df in dfs_docente_etapa_ensino.items()
        ]
    )
    .assign(
        sigla_uf=lambda d: (
            d["uf"]
            .str.strip()
            .replace(
                {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}
            )
        ),
        id_municipio=lambda d: d["id_municipio"].astype("string"),
        quantidade_docentes=lambda d: d["quantidade_docentes"].astype("Int64"),
    )
    .rename(columns={"variable": "etapa_ensino"})[
        [
            "id_municipio",
            "etapa_ensino",
            "quantidade_docentes",
            "tipo_classe",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_docente_etapa_ensino.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "docente_etapa_ensino" / "ano=2025" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )


## Docente Localizacao

sheets_docente_localizacao = {
    "Educação Básica": "2.2",
    "Educação Infantil": "2.9",
    "Educação Infantil - Creche": "Creche 2.10",
    "Educação Infantil - Pré-Escola": "Pré-Escola 2.15",
    "Ensino Fundamental": "2.21",
    "Ensino Fundamental - Anos Iniciais": "Anos Iniciais 2.26",
    "Ensino Fundamental - Anos Finais": "Anos Finais 2.31",
    "Ensino Médio": "2.38",
    "Educação Profissional": "2.44",
    "EJA": "2.50",
    "Educação Especial - Classes Comuns": "2.63",
    "Educação Especial - Classes Exclusivas": "2.70",
}

# RENAMES_DOCENTE_LOCALIZACAO = {
#     "Educação Básica": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Unnamed: 16": "urbana_federal",
#         "Unnamed: 17": "urbana_estadual",
#         "Unnamed: 18": "urbana_municipal",
#         "Unnamed: 19": "urbana_privada",
#         "Unnamed: 26": "rural_federal",
#         "Unnamed: 27": "rural_estadual",
#         "Unnamed: 28": "rural_municipal",
#         "Unnamed: 29": "rural_privada",
#     },
#     "Educação Infantil": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Educação Infantil - Creche": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": "Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Educação Infantil - Pré-Escola": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Ensino Fundamental": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": " Total_Pública",
#         "Federal": " Total_Federal",
#         "Estadual": " Total_Estadual",
#         "Municipal": " Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Ensino Fundamental - Anos Iniciais": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": "Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Ensino Fundamental - Anos Finais": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Ensino Médio": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Educação Profissional": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": " Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "EJA": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": "Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Educação Especial - Classes Comuns": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": "Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
#     "Educação Especial - Classes Exclusivas": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Pública": "Total_Pública",
#         "Federal": "Total_Federal",
#         "Estadual": "Total_Estadual",
#         "Municipal": "Total_Municipal",
#         "Privada": "Total_Privada",
#         "Pública.1": "Urbana_Pública",
#         "Federal.1": "Urbana_Federal",
#         "Estadual.1": "Urbana_Estadual",
#         "Municipal.1": "Urbana_Municipal",
#         "Privada.1": "Urbana_Privada",
#         "Pública.2": "Rural_Pública",
#         "Federal.2": "Rural_Federal",
#         "Estadual.2": "Rural_Estadual",
#         "Municipal.2": "Rural_Municipal",
#         "Privada.2": "Rural_Privada",
#     },
# }

dfs_docente_localizacao = {
    name: drop_unused_columns(
        read_sheet(sheet_name, skiprows=10).rename(
            columns={
                "Unnamed: 1": "uf",
                "Unnamed: 3": "id_municipio",
                "Unnamed: 16": "urbana_federal",
                "Unnamed: 17": "urbana_estadual",
                "Unnamed: 18": "urbana_municipal",
                "Unnamed: 19": "urbana_privada",
                "Unnamed: 26": "rural_federal",
                "Unnamed: 27": "rural_estadual",
                "Unnamed: 28": "rural_municipal",
                "Unnamed: 29": "rural_privada",
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_docente_localizacao.items()
}


# dfs_docente_localizacao: dict[str, pd.DataFrame] = {
#     name: read_sheet(sheet_name)
#     for name, sheet_name in sheets_docente_localizacao.items()
# }
#
# dfs_docente_localizacao = {
#     name: drop_unused_columns(
#         df.rename(columns=RENAMES_DOCENTE_LOCALIZACAO[name], errors="raise")
#     )
#     for name, df in dfs_docente_localizacao.items()
# }

df_docente_localizacao = pd.concat(
    [
        df.assign(etapa_ensino=etapa_ensino)
        .pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
            ]
        )
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=[
                    "id_municipio",
                    "uf",
                    "etapa_ensino",
                ],
                value_vars=[
                    c
                    for c in d.columns
                    if c not in ["id_municipio", "uf", "etapa_ensino"]
                ],
                value_name="quantidade_docente",
            )
        )
        for etapa_ensino, df in dfs_docente_localizacao.items()
    ]
).assign(
    sigla_uf=lambda d: (
        d["uf"]
        .str.strip()
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    ),
    id_municipio=lambda d: d["id_municipio"].astype("string"),
    quantidade_docente=lambda d: d["quantidade_docente"].astype("Int64"),
    rede=lambda d: d["variable"].apply(lambda v: v.split("_")[-1].strip()),
    localizacao=lambda d: d["variable"].apply(
        lambda v: v.split("_")[0].strip()
    ),
)[
    [
        "id_municipio",
        "etapa_ensino",
        "rede",
        "localizacao",
        "quantidade_docente",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_docente_localizacao.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "docente_localizacao" / "ano=2025" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

## Docente Escolaridade

sheets_docente_escolaridade = {
    "Educacao Basica": "2.5",
    "Educacao Infantil - Creche": "2.13",
    "Educacao Infantil - Pré-Escola": "2.18",
    "Ensino Fundamental": "2.24",
    "Ensino Fundamental - Anos Iniciais": "2.29",
    "Ensino Fundamental - Anos Finais": "2.34",
    "Ensino Médio": "2.41",
    "Educacao Profissional": "2.47",
    "EJA": "2.53",
    "Educacao Especial - Classes Comuns": "2.66",
    "Educacao Especial - Classes Exclusivas": "2.73",
}

RENAMES_DOCENTE_ESCOLARIDADE = {
    "Educacao Basica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura10": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Educacao Infantil - Creche": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura10": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Educacao Infantil - Pré-Escola": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura10": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Ensino Fundamental - Anos Iniciais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Ensino Fundamental - Anos Finais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Ensino Médio": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Educacao Profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "EJA": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Educacao Especial - Classes Comuns": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura9": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
    "Educacao Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Unnamed: 5": "Ensino Fundamental",
        "Unnamed: 6": "Ensino Médio",
        "Com Licenciatura8": "Graduação - Com Licenciatura",
        "Sem Licenciatura": "Graduação - Sem Licenciatura",
        "Especialização": "Pós Graduação - Especialização",
        "Mestrado": "Pós Graduação - Mestrado",
        "Doutorado": "Pós Graduação - Doutorado",
    },
}

dfs_docente_escolaridade = {
    name: drop_unused_columns(
        read_sheet(sheet_name, skiprows=10).rename(
            columns={
                "Unnamed: 1": "uf",
                "Unnamed: 3": "id_municipio",
                "Unnamed: 5": "Ensino Fundamental",
                "Unnamed: 6": "Ensino Médio",
                "Unnamed: 8": "Graduação - Com Licenciatura",
                "Unnamed: 9": "Graduação - Sem Licenciatura",
                "Unnamed: 11": "Pós Graduação - Especialização",
                "Unnamed: 12": "Pós Graduação - Mestrado",
                "Unnamed: 13": "Pós Graduação - Doutorado",
            },
            errors="raise",
        )
    )
    for name, sheet_name in sheets_docente_escolaridade.items()
}

# dfs_docente_escolaridade: dict[str, pd.DataFrame] = {
#     name: read_sheet(sheet_name, skiprows=9)
#     for name, sheet_name in sheets_docente_escolaridade.items()
# }
#
# dfs_docente_escolaridade = {
#     name: drop_unused_columns(
#         df.rename(columns=RENAMES_DOCENTE_ESCOLARIDADE[name], errors="raise")
#     )
#     for name, df in dfs_docente_escolaridade.items()
# }

df_docente_escolaridade = (
    pd.concat(
        [
            df.assign(etapa_ensino=etapa_ensino)
            .pipe(
                lambda d: d.loc[
                    (d["id_municipio"].notna())
                    & (d["id_municipio"].str.strip() != ""),
                ]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf", "etapa_ensino"],
                    value_vars=[
                        c
                        for c in d.columns
                        if c not in ["id_municipio", "uf", "etapa_ensino"]
                    ],
                    value_name="quantidade_docente",
                )
            )
            for etapa_ensino, df in dfs_docente_escolaridade.items()
        ]
    )
    .assign(
        sigla_uf=lambda d: (
            d["uf"]
            .str.strip()
            .replace(
                {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}
            )
        ),
        id_municipio=lambda d: d["id_municipio"].astype("string"),
        quantidade_docente=lambda d: d["quantidade_docente"].astype("Int64"),
    )
    .rename(
        columns={"variable": "escolaridade", "etapa_ensino": "tipo_classe"}
    )[
        [
            "id_municipio",
            "escolaridade",
            "quantidade_docente",
            "tipo_classe",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_docente_escolaridade.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "docente_escolaridade" / "ano=2025" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )


## Docente deficiência

sheets_docente_deficiencia = {
    "Educacao Especial - Classes Comuns": "2.67",
    "Educacao Especial - Classes Exclusivas": "2.74",
}

RENAMES_DOCENTE_DEFICIENCIA = {
    "Educacao Especial - Classes Comuns": {
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
    },
    "Educacao Especial - Classes Exclusivas": {
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
    },
}

dfs_docente_deficiencia: dict[str, pd.DataFrame] = {
    name: read_sheet(sheet_name, skiprows=7)
    for name, sheet_name in sheets_docente_deficiencia.items()
}

dfs_docente_deficiencia = {
    name: drop_unused_columns(
        df.rename(columns=RENAMES_DOCENTE_DEFICIENCIA[name], errors="raise")
    )
    for name, df in dfs_docente_deficiencia.items()
}


df_docente_deficiencia = (
    pd.concat(
        [
            df.assign(etapa_ensino=etapa_ensino)
            .pipe(
                lambda d: d.loc[
                    (d["id_municipio"].notna())
                    & (d["id_municipio"].str.strip() != ""),
                ]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf", "etapa_ensino"],
                    value_vars=[
                        c
                        for c in d.columns
                        if c not in ["id_municipio", "uf", "etapa_ensino"]
                    ],
                    value_name="quantidade_docente",
                )
            )
            for etapa_ensino, df in dfs_docente_deficiencia.items()
        ]
    )
    .assign(
        sigla_uf=lambda d: (
            d["uf"]
            .str.strip()
            .replace(
                {i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")}
            )
        ),
        id_municipio=lambda d: d["id_municipio"].astype("string"),
        quantidade_docente=lambda d: d["quantidade_docente"].astype("Int64"),
    )
    .rename(
        columns={"variable": "deficiencia", "etapa_ensino": "tipo_classe"}
    )[
        [
            "id_municipio",
            "deficiencia",
            "quantidade_docente",
            "tipo_classe",
            "sigla_uf",
        ]
    ]
)

for sigla_uf, df in df_docente_deficiencia.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT / "docente_deficiencia" / "ano=2025" / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

## Docente faixa etária sexo

sheets_docente_faixa_etaria_sexo = {
    "Educação Básica": "2.4",
    "Educação Infantil - Creche": "2.12",
    "Educação Infantil - Pré-Escola": "2.17",
    "Ensino Fundamental": "2.23",
    "Ensino Fundamental - Anos Iniciais": "2.28",
    "Ensino Fundamental - Anos Finais": "2.33",
    "Ensino Médio": "2.40",
    "Educação Profissional": "2.46",
    "EJA": "2.52",
    "Educação Especial - Classes Comuns": "2.65",
    "Educação Especial - Classes Exclusivas": "2.72",
}

RENAMES_DOCENTE_FAIXA_ETARIA_SEXO = {
    "Educação Básica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Educação Infantil - Creche": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Educação Infantil - Pré-Escola": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Ensino Fundamental - Anos Iniciais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Ensino Fundamental - Anos Finais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Ensino Médio": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Educação Profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "EJA": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Educação Especial - Classes Comuns": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
    "Educação Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Até 24 anos": "Feminino_Até 24 anos",
        "De 25 a 29 anos": "Feminino_25 a 29 anos",
        "De 30 a 39 anos": "Feminino_30 a 39 anos",
        "De 40 a 49 anos": "Feminino_40 a 49 anos",
        "De 50 a 54 anos": "Feminino_50 a 54 anos",
        "De 55 a 59 anos": "Feminino_55 a 59 anos",
        "60 anos ou mais": "Feminino_60 anos ou mais",
        "Até 24 anos.1": "Masculino_Até 24 anos",
        "De 25 a 29 anos.1": "Masculino_25 a 29 anos",
        "De 30 a 39 anos.1": "Masculino_30 a 39 anos",
        "De 40 a 49 anos.1": "Masculino_40 a 49 anos",
        "De 50 a 54 anos.1": "Masculino_50 a 54 anos",
        "De 55 a 59 anos.1": "Masculino_55 a 59 anos",
        "60 anos ou mais.1": "Masculino_60 anos ou mais",
    },
}

dfs_docente_faixa_etaria_sexo: dict[str, pd.DataFrame] = {
    name: read_sheet(sheet_name)
    for name, sheet_name in sheets_docente_faixa_etaria_sexo.items()
}

dfs_docente_faixa_etaria_sexo = {
    name: drop_unused_columns(
        df.rename(
            columns=RENAMES_DOCENTE_FAIXA_ETARIA_SEXO[name], errors="raise"
        )
    )
    for name, df in dfs_docente_faixa_etaria_sexo.items()
}


df_docente_faixa_etaria_sexo = pd.concat(
    [
        df.assign(etapa_ensino=etapa_ensino)
        .pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna())
                & (d["id_municipio"].str.strip() != ""),
            ]
        )
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=["id_municipio", "uf", "etapa_ensino"],
                value_vars=[
                    c
                    for c in d.columns
                    if c not in ["id_municipio", "uf", "etapa_ensino"]
                ],
                value_name="quantidade_docente",
            )
        )
        for etapa_ensino, df in dfs_docente_faixa_etaria_sexo.items()
    ]
).assign(
    sigla_uf=lambda d: (
        d["uf"]
        .str.strip()
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    ),
    id_municipio=lambda d: d["id_municipio"].astype("string"),
    quantidade_docente=lambda d: d["quantidade_docente"].astype("Int64"),
    faixa_etaria=lambda d: d["variable"].apply(
        lambda v: (
            v.replace("Feminino_", "")
            if v.startswith("Feminino")
            else v.replace("Masculino_", "")
        )
    ),
    sexo=lambda d: d["variable"].apply(lambda v: v.split("_")[0].strip()),
)[
    [
        "id_municipio",
        "faixa_etaria",
        "quantidade_docente",
        "etapa_ensino",
        "sexo",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_docente_faixa_etaria_sexo.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT
        / "docente_faixa_etaria_sexo"
        / "ano=2025"
        / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

## Docente regime contrato

sheets_docente_regime_contrato = {
    "Educação Basica": "2.6",
    "Educação Infantil - Creche": "2.14",
    "Educação Infantil - Pré-Escola": "2.19",
    "Ensino Fundamental - Anos Iniciais": "2.30",
    "Ensino Fundamental - Anos Finais": "2.35",
    "Ensino Médio": "2.42",
    "Educação Profissional": "2.48",
    "EJA": "2.54",
    "Educação Especial - Classes Comuns": "2.68",
    "Educação Especial - Classes Exclusivas": "2.75",
}

RENAMES_DOCENTE_REGIME_CONTRATO = {
    "Educação Basica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Educação Infantil - Creche": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Educação Infantil - Pré-Escola": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Ensino Fundamental - Anos Iniciais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Ensino Fundamental - Anos Finais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Ensino Médio": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Educação Profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "EJA": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Educação Especial - Classes Comuns": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
    "Educação Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Federal": "Concursado_Federal",
        "Estadual": "Concursado_Estadual",
        "Municipal": "Concursado_Municipal",
        "Federal.1": "Contrato Temporário_Federal",
        "Estadual.1": "Contrato Temporário_Estadual",
        "Municipal.1": "Contrato Temporário_Municipal",
        "Federal.2": "Contrato Terceirizado_Federal",
        "Estadual.2": "Contrato Terceirizado_Estadual",
        "Municipal.2": "Contrato Terceirizado_Municipal",
        "Federal.3": "Contrato CLT_Federal",
        "Estadual.3": "Contrato CLT_Estadual",
        "Municipal.3": "Contrato CLT_Municipal",
    },
}


dfs_docente_regime_contrato: dict[str, pd.DataFrame] = {
    name: read_sheet(sheet_name)
    for name, sheet_name in sheets_docente_regime_contrato.items()
}

dfs_docente_regime_contrato = {
    name: drop_unused_columns(
        df.rename(
            columns=RENAMES_DOCENTE_REGIME_CONTRATO[name], errors="raise"
        )
    )
    for name, df in dfs_docente_regime_contrato.items()
}


df_docente_regime_contrato = pd.concat(
    [
        df.assign(etapa_ensino=etapa_ensino)
        .pipe(
            lambda d: d.loc[
                (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
            ]
        )
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=["id_municipio", "uf", "etapa_ensino"],
                value_vars=[
                    c
                    for c in d.columns
                    if c not in ["id_municipio", "uf", "etapa_ensino"]
                ],
                value_name="quantidade_docente",
            )
        )
        for etapa_ensino, df in dfs_docente_regime_contrato.items()
    ]
).assign(
    sigla_uf=lambda d: (
        d["uf"]
        .str.strip()
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    ),
    id_municipio=lambda d: d["id_municipio"].astype("string"),
    quantidade_docente=lambda d: d["quantidade_docente"].astype("Int64"),
    rede=lambda d: d["variable"].apply(lambda v: v.split("_")[-1].strip()),
    regime_contrato=lambda d: d["variable"].apply(
        lambda v: v.split("_")[0].strip()
    ),
)[
    [
        "id_municipio",
        "regime_contrato",
        "quantidade_docente",
        "etapa_ensino",
        "rede",
        "sigla_uf",
    ]
]

for sigla_uf, df in df_docente_regime_contrato.groupby("sigla_uf"):
    save_path_uf = (
        OUTPUT
        / "docente_regime_contrato"
        / "ano=2025"
        / f"sigla_uf={sigla_uf}"
    )
    os.makedirs(save_path_uf, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        (save_path_uf / "data.csv"), index=False
    )

## Subir tabelas

for dir in OUTPUT.iterdir():
    table_id = dir.name
    tb = bd.Table(
        dataset_id="br_inep_sinopse_estatistica_educacao_basica",
        table_id=table_id,
    )
    tb.create(
        path=dir,
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )
