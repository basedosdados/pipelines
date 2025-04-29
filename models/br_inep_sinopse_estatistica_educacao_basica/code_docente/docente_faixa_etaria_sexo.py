# -*- coding: utf-8 -*-
import os
import zipfile
import pandas as pd
import basedosdados as bd
import numpy as np


INPUT = os.path.join(os.getcwd(), "br_inep_sinopse_estatistica_educacao_basica/input")
OUTPUT = os.path.join(os.getcwd(), "br_inep_sinopse_estatistica_educacao_basica/output")

# os.makedirs(INPUT, exist_ok=True)
# os.makedirs(OUTPUT, exist_ok=True)

RENAMES_ETAPA_ENSINO_SERIE = {
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


localizacao = {
    "educacao_basica": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Básica"],
        "chave": "2.3",
        "valor": "Educação Básica",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "educacao_infantil_creche": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Infantil - Creche"],
        "chave": "2.9",
        #"chave": "2.8", # para anos anteriores a 2010
        "valor": "Educação Infantil - Creche",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "educacao_infantil_pre_escola": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Infantil - Pré-Escola"],
        "chave": "2.13",
        #"chave": "2.11", # Para anos anteriores a 2010
        "valor": "Educação Infantil - Pré-Escola",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "ensino_fundamental": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental"],
        "chave": "2.18",
        #"chave": "2.15", # Para anos anteriores a 2010
        "valor": "Ensino Fundamental",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "ensino_fundamental_anos_iniciais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental - Anos Iniciais"],
        "chave": "2.22",
        #"chave": "2.18", # Para anos anteriores a 2010
        "valor": "Ensino Fundamental - Anos Iniciais",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "ensino_fundamental_anos_finais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental - Anos Finais"],
        "chave": "2.26",
        #"chave": "2.21", # Para anos anteriores a 2010
        "valor": "Ensino Fundamental - Anos Finais",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "ensino_medio": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Médio"],
        "chave": "2.30",
        #"chave": "2.24", # Para anos anteriores a 2010
        "valor": "Ensino Médio",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "educacao_profissional": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Profissional"],
        "chave": "2.35",
        #"chave": "2.28", # Para anos anteriores a 2010
        "valor": "Educação Profissional",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "EJA": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["EJA"],
        "chave": "2.40",
        #"chave": "2.32", # Para anos anteriores a 2010
        "valor": "EJA",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "educacao_especial_classes_comuns": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Especial - Classes Comuns"],
        "chave": "2.46",
        #"chave": "2.37", # Para anos anteriores a 2010
        "valor": "Educação Especial - Classes Comuns",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Especial - Classes Exclusivas"],
        "chave": "2.52",
        #"chave": "2.51", # Para o ano de 2011
        #"chave": "2.41", # Para o ano anteriores a 2010
        "valor": "Educação Especial - Classes Exclusivas",
        "skiprows": 8,
        "table": "docente_faixa_etaria_sexo",
    },
}


def read_sheet(
    table: str, ano: int, chave: str, valor: str, dicionario: dict, skiprows: int = 9
) -> pd.DataFrame:
    print("Tratando dados de", valor, ano)
    path_excel = os.path.join(
        INPUT,
        # f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
        # f"Sinopse_Estatistica_da_Educaç╞o_Basica_{ano}.xlsx",
        f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
        f"Sinopse_Estatistica_da_Educação_Basica_{ano}.xlsx"
    )

    df = pd.read_excel(
        path_excel,
        skiprows=skiprows,
        sheet_name=chave,
    )

    sheets_etapa_ensino_serie = {chave: valor}

    dfs_faixa_etaria = {
        name: pd.read_excel(
            path_excel,
            skiprows=skiprows,
            sheet_name=sheet_name,
        )
        for sheet_name, name in sheets_etapa_ensino_serie.items()
    }

    dataframes = {}
    for table_name, columns in dfs_faixa_etaria.items():
        df = pd.DataFrame(columns)
        dataframes[table_name] = df

    print(df.columns)

    def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
        cols_drop = [
            col
            for col in df.columns
            if col.startswith("Unnamed") or col.startswith("Total")
        ]

        return df.drop(columns=cols_drop)

    dfs_faixa_etaria = {
        name: drop_unused_columns(
            df.rename(columns=RENAMES_ETAPA_ENSINO_SERIE[name], errors="raise")
        )
        for name, df in dfs_faixa_etaria.items()
    }

    df_faixa_etaria = pd.concat(
        [
            df.pipe(
                lambda d: d.loc[(d["id_municipio"].notna()) & (d["id_municipio"] != " "),]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf"],
                    value_vars=d.columns.difference(
                        ["id_municipio", "uf"]
                    ).tolist(),  # Convert to list
                    var_name="faixa_etaria",
                    value_name="quantidade_docente",
                )
            )
            .assign(etapa_ensino=etapa_ensino)
            for etapa_ensino, df in dfs_faixa_etaria.items()
        ]
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_faixa_etaria["uf"] = (
        df_faixa_etaria["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})  # type: ignore
    )

    df_faixa_etaria = df_faixa_etaria.rename(columns={"uf": "sigla_uf"}, errors="raise")

    df_faixa_etaria["sexo"] = df_faixa_etaria["faixa_etaria"].apply(
        lambda v: v.split("_")[0]
    )
    df_faixa_etaria["faixa_etaria"] = df_faixa_etaria["faixa_etaria"].apply(
        lambda v: v.split("_")[-1]
    )


    df_faixa_etaria["quantidade_docente"] = df_faixa_etaria["quantidade_docente"].astype(
        int
    )

    print("Particionando dados")
    for sigla_uf, df in df_faixa_etaria.groupby("sigla_uf"):
        path = os.path.join(OUTPUT, f"{table}", f"ano={ano}", f"sigla_uf={sigla_uf}")
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"), index=False, mode="w"
            )
        else:
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"), index=False, mode="a", header=False
            )

if __name__ == '__main__' :
    lista = [
        "educacao_basica",
        "educacao_infantil_creche",
        "educacao_infantil_pre_escola",
        "ensino_fundamental",
        "ensino_fundamental_anos_iniciais",
        "ensino_fundamental_anos_finais",
        "ensino_medio",
        "educacao_profissional",
        "EJA",
        "educacao_especial_classes_comuns",
        "educacao_especial_classes_exclusivas",
    ]

    for x in lista:
        read_sheet(
            table=localizacao[x]["table"],
            ano=2023,
            chave=localizacao[x]["chave"],
            valor=localizacao[x]["valor"],
            dicionario=localizacao[x]["dicionario"],
            skiprows=localizacao[x]["skiprows"],
        )
