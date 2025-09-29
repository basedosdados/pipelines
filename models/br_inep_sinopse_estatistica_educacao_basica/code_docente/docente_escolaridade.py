import os

import basedosdados as bd
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

# os.makedirs(INPUT, exist_ok=True)
# os.makedirs(OUTPUT, exist_ok=True)

RENAMES_ETAPA_ENSINO_SERIE = {
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


escolaridade = {
    "educacao_basica": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Basica"],
        "chave": "2.4",
        "valor": "Educacao Basica",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "ensino_infantil_creche": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Infantil - Creche"],
        # "chave": "2.10",
        "chave": "2.9",  # Para anos anteriores a 2010
        "valor": "Educacao Infantil - Creche",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "educacao_infantil_pre_escola": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educacao Infantil - Pré-Escola"
        ],
        # "chave": "2.14",
        "chave": "2.12",  # Para anos anteriores a 2010
        "valor": "Educacao Infantil - Pré-Escola",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "ensino_fundamental": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental"],
        "chave": "2.19",
        "chave": "2.16",  # Para anos anteriores a 2010  # noqa: F601
        "valor": "Ensino Fundamental",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "ensino_fundamental_anos_iniciais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Ensino Fundamental - Anos Iniciais"
        ],
        # "chave": "2.23",
        "chave": "2.19",  # Para anos anteriores a 2010
        "valor": "Ensino Fundamental - Anos Iniciais",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "ensino_fundamental_anos_finais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Ensino Fundamental - Anos Finais"
        ],
        # "chave": "2.27",
        "chave": "2.22",  # Para anos anteriores a 2010
        "valor": "Ensino Fundamental - Anos Finais",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "ensino_medio": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Médio"],
        # "chave": "2.31",
        "chave": "2.25",  # Para anos anteriores a 2010
        "valor": "Ensino Médio",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "educacao_profissional": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Profissional"],
        # "chave": "2.36",
        "chave": "2.29",  # Para anos anteriores a 2010
        "valor": "Educacao Profissional",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "EJA": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["EJA"],
        # "chave": "2.41",
        "chave": "2.33",  # Para anos anteriores a 2010
        "valor": "EJA",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "educacao_especial_classes_comuns": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educacao Especial - Classes Comuns"
        ],
        # "chave": "2.47",
        "chave": "2.38",  # Para anos anteriores a 2010
        "valor": "Educacao Especial - Classes Comuns",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educacao Especial - Classes Exclusivas"
        ],
        # "chave": "2.53",
        "chave": "2.52",  # Para o ano de 2011
        "chave": "2.42",  # Para anos anteriores a 2010  # noqa: F601
        "valor": "Educacao Especial - Classes Exclusivas",
        "skiprows": 9,
        "table": "docente_escolaridade",
    },
}


def read_sheet(
    table: str,
    ano: int,
    chave: str,
    valor: str,
    dicionario: dict,
    skiprows: int = 9,
) -> pd.DataFrame:
    print("Tratando dados de", valor, ano)
    path_excel = os.path.join(
        INPUT,
        f"Sinopse_Estatistica_da_Educaç╞o_Basica_{ano}",
        f"Sinopse_Estatistica_da_Educaç╞o_Basica_{ano}.xlsx",
    )
    df = pd.read_excel(
        path_excel,
        skiprows=skiprows,
        sheet_name=chave,
    )

    sheets_escolaridade = {chave: valor}

    dfs_escolaridade = {
        name: pd.read_excel(
            path_excel,
            skiprows=skiprows,
            sheet_name=sheet_name,
        )
        for sheet_name, name in sheets_escolaridade.items()
    }

    dataframes = {}
    for table_name, columns in dfs_escolaridade.items():
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

    dfs_escolaridade = {
        name: drop_unused_columns(
            df.rename(columns=dicionario, errors="raise")
        )
        for name, df in dfs_escolaridade.items()
    }

    df_escolaridade = pd.concat(
        [
            df.pipe(
                lambda d: d.loc[
                    (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
                ]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf"],
                    value_vars=d.columns.difference(
                        ["id_municipio", "uf"]
                    ).tolist(),  # Convert to list
                    var_name="escolaridade",
                    value_name="quantidade_docente",
                )
            )
            .assign(tipo_classe=tipo_classe)
            for tipo_classe, df in dfs_escolaridade.items()
        ]
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_escolaridade["uf"] = (
        df_escolaridade["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    )  # type: ignore

    df_escolaridade = df_escolaridade.rename(
        columns={"uf": "sigla_uf"}, errors="raise"
    )

    print("Particionando dados")
    for sigla_uf, df in df_escolaridade.groupby("sigla_uf"):
        path = os.path.join(
            OUTPUT, f"{table}", f"ano={ano}", f"sigla_uf={sigla_uf}"
        )
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"), index=False, mode="w"
            )
        else:
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"),
                index=False,
                mode="a",
                header=False,
            )


lista = [
    "educacao_basica",
    "ensino_infantil_creche",
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
        table=escolaridade[x]["table"],
        ano=2007,
        chave=escolaridade[x]["chave"],
        valor=escolaridade[x]["valor"],
        dicionario=escolaridade[x]["dicionario"],
        skiprows=escolaridade[x]["skiprows"],
    )
