import os

import basedosdados as bd
import pandas as pd

INPUT = os.path.join(
    os.getcwd(), "br_inep_sinopse_estatistica_educacao_basica/input"
)
OUTPUT = os.path.join(
    os.getcwd(), "br_inep_sinopse_estatistica_educacao_basica/output"
)

# os.makedirs(INPUT, exist_ok=True)
# os.makedirs(OUTPUT, exist_ok=True)

#####
# Para os anos posteriores a 2011
#####

RENAMES_CONTRATO = {
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

# RENAMES_CONTRATO = { # Para o ano de 2011
#     "Educação Basica": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Educação Infantil - Creche": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Educação Infantil - Pré-Escola": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Ensino Fundamental": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Ensino Fundamental - Anos Iniciais": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Ensino Fundamental - Anos Finais": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Ensino Médio": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Educação Profissional": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "EJA": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Educação Especial - Classes Comuns": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
#     "Educação Especial - Classes Exclusivas": {
#         "Unnamed: 1": "uf",
#         "Unnamed: 3": "id_municipio",
#         "Federal": "Concursado_Federal",
#         "Estadual": "Concursado_Estadual",
#         "Municipal": "Concursado_Municipal",
#         "Federal.1": "Contrato Temporário_Federal",
#         "Estadual.1": "Contrato Temporário_Estadual",
#         "Municipal.1": "Contrato Temporário_Municipal",
#         "Federal.2": "Contrato Terceirizado_Federal",
#         "Estadual.2": "Contrato Terceirizado_Estadual",
#         "Municipal.2": "Contrato Terceirizado_Municipal",

#     },
# }


regime_contrato = {
    "educacao_basica": {
        "dicionario": RENAMES_CONTRATO["Educação Basica"],
        "chave": "2.5",
        "valor": "Educação Basica",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "ensino_infantil_creche": {
        "dicionario": RENAMES_CONTRATO["Educação Infantil - Creche"],
        "chave": "2.11",
        "valor": "Educação Infantil - Creche",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "educacao_infantil_pre_escola": {
        "dicionario": RENAMES_CONTRATO["Educação Infantil - Pré-Escola"],
        "chave": "2.15",
        "valor": "Educação Infantil - Pré-Escola",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "ensino_fundamental": {
        "dicionario": RENAMES_CONTRATO["Ensino Fundamental"],
        "chave": "2.20",
        "valor": "Ensino Fundamental",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "ensino_fundamental_anos_iniciais": {
        "dicionario": RENAMES_CONTRATO["Ensino Fundamental - Anos Iniciais"],
        "chave": "2.24",
        "valor": "Ensino Fundamental - Anos Iniciais",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "ensino_fundamental_anos_finais": {
        "dicionario": RENAMES_CONTRATO["Ensino Fundamental - Anos Finais"],
        "chave": "2.28",
        "valor": "Ensino Fundamental - Anos Finais",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "ensino_medio": {
        "dicionario": RENAMES_CONTRATO["Ensino Médio"],
        "chave": "2.32",
        "valor": "Ensino Médio",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "educacao_profissional": {
        "dicionario": RENAMES_CONTRATO["Educação Profissional"],
        "chave": "2.37",
        "valor": "Educação Profissional",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "EJA": {
        "dicionario": RENAMES_CONTRATO["EJA"],
        "chave": "2.42",
        "valor": "EJA",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "educacao_especial_classes_comuns": {
        "dicionario": RENAMES_CONTRATO["Educação Especial - Classes Comuns"],
        "chave": "2.49",
        # "chave": "2.48",  # Para o ano de 2011
        "valor": "Educação Especial - Classes Comuns",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        "table": "docente_regime_contrato",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAMES_CONTRATO[
            "Educação Especial - Classes Exclusivas"
        ],
        "chave": "2.55",
        # "chave": "2.53",  # Para o ano de 2011
        "valor": "Educação Especial - Classes Exclusivas",
        "skiprows": 8,
        # "skiprows": 9,  # Até o ano de 2021
        # "skiprows": 10,  # Para o ano de 2011
        "table": "docente_regime_contrato",
    },
}


def read_sheet(
    table: str, ano: int, chave: str, valor: str, dicionario: dict, skiprows
) -> pd.DataFrame:
    print("Tratando dados de", valor, ano)
    path_excel = os.path.join(
        INPUT,
        # f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
        # f"Sinopse_Estatistica_da_Educaç╞o_Basica_{ano}.xlsx",
        f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
        f"Sinopse_Estatistica_da_Educação_Basica_{ano}.xlsx",
    )

    df = pd.read_excel(
        path_excel,
        skiprows=skiprows,
        sheet_name=chave,
    )

    sheets_etapa_ensino_serie = {chave: valor}

    dfs_regime_contrato = {
        name: pd.read_excel(
            path_excel, skiprows=skiprows, sheet_name=sheet_name
        )
        for sheet_name, name in sheets_etapa_ensino_serie.items()
    }

    dataframes = {}

    for table_name, columns in dfs_regime_contrato.items():
        df = pd.DataFrame(columns)  # Create DataFrame for each table
        dataframes[table_name] = df  # Store the DataFrame in a dictionary

    print(df.columns)

    def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
        cols_drop = [
            col
            for col in df.columns
            if col.startswith("Unnamed") or col.startswith("Total")
        ]

        return df.drop(columns=cols_drop)

    dfs_regime_contrato = {
        name: drop_unused_columns(
            df.rename(columns=dicionario, errors="raise")
        )
        for name, df in dfs_regime_contrato.items()
    }

    df_regime_contrato = pd.concat(
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
                    var_name="regime_contrato",
                    value_name="quantidade_docente",
                )
            )
            .assign(etapa_ensino=etapa_ensino)
            for etapa_ensino, df in dfs_regime_contrato.items()
        ]
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_regime_contrato["uf"] = (
        df_regime_contrato["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    )  # type: ignore

    df_regime_contrato = df_regime_contrato.rename(
        columns={"uf": "sigla_uf"}, errors="raise"
    )

    df_regime_contrato["rede"] = df_regime_contrato["regime_contrato"].apply(
        lambda v: v.split("_")[-1]
    )

    df_regime_contrato["regime_contrato"] = df_regime_contrato[
        "regime_contrato"
    ].apply(lambda v: v.split("_")[0])

    df_regime_contrato["quantidade_docente"] = df_regime_contrato[
        "quantidade_docente"
    ].astype(int)

    df_regime_contrato["quantidade_docente"] = df_regime_contrato[
        "quantidade_docente"
    ].astype(int)

    print("Particionando dados")
    for sigla_uf, df in df_regime_contrato.groupby("sigla_uf"):
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


if __name__ == "__main__":
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
        # for ano in range(2012, 2019):
        read_sheet(
            table=regime_contrato[x]["table"],
            ano=2023,
            chave=regime_contrato[x]["chave"],
            valor=regime_contrato[x]["valor"],
            dicionario=regime_contrato[x]["dicionario"],
            skiprows=regime_contrato[x]["skiprows"],
        )
