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

RENAMES_ETAPA_ENSINO_SERIE = {
    "Educação Basica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Infantil": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Infantil - Creche": {
        # "Unnamed: 1": "uf",
        # "Unnamed: 3": "id_municipio",
        # "Pública": " Total_Pública",
        # "Federal": " Total_Federal",
        # "Estadual": " Total_Estadual",
        # "Municipal": " Total_Municipal",
        # "Privada": " Total_Privada",
        # "Pública.1": "Urbana_Pública",
        # "Federal.1": "Urbana_Federal",
        # "Estadual.1": "Urbana_Estadual",
        # "Municipal.1": "Urbana_Municipal",
        # "Privada.1": "Urbana_Privada",
        # "Pública.2": "Rural_Pública",
        # "Federal.2": "Rural_Federal",
        # "Estadual.2": "Rural_Estadual",
        # "Municipal.2": "Rural_Municipal",
        # "Privada.2": "Rural_Privada",
        ####
        # Para 2014
        ####
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública": "Urbana_Pública",  # noqa: F601
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.1": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Infantil - Pré-Escola": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Ensino Fundamental - Anos Iniciais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Ensino Fundamental - Anos Finais": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Ensino Médio": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Profissional": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "EJA": {
        # "Unnamed: 1": "uf",
        # "Unnamed: 3": "id_municipio",
        # "Pública": " Total_Pública",
        # "Federal": " Total_Federal",
        # "Estadual": " Total_Estadual",
        # "Municipal": " Total_Municipal",
        # "Privada": " Total_Privada",
        # "Pública.1": "Urbana_Pública",
        # "Federal.1": "Urbana_Federal",
        # "Estadual.1": "Urbana_Estadual",
        # "Municipal.1": "Urbana_Municipal",
        # "Privada.1": "Urbana_Privada",
        # "Pública.2": "Rural_Pública",
        # "Federal.2": "Rural_Federal",
        # "Estadual.2": "Rural_Estadual",
        # "Municipal.2": "Rural_Municipal",
        # "Privada.2": "Rural_Privada",
        ####
        # Para 2014
        ####
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Especial - Classes Comuns": {
        # "Unnamed: 1": "uf",
        # "Unnamed: 3": "id_municipio",
        # "Pública": " Total_Pública",
        # "Federal": " Total_Federal",
        # "Estadual": " Total_Estadual",
        # "Municipal": " Total_Municipal",
        # "Privada": " Total_Privada",
        # "Pública.1": "Urbana_Pública",
        # "Federal.1": "Urbana_Federal",
        # "Estadual.1": "Urbana_Estadual",
        # "Municipal.1": "Urbana_Municipal",
        # "Privada.1": "Urbana_Privada",
        # "Pública.2": "Rural_Pública",
        # "Federal.2": "Rural_Federal",
        # "Estadual.2": "Rural_Estadual",
        # "Municipal.2": "Rural_Municipal",
        # "Privada.2": "Rural_Privada",
        #####
        # Para 2014
        ####
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
    "Educação Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": " Total_Pública",
        "Federal": " Total_Federal",
        "Estadual": " Total_Estadual",
        "Municipal": " Total_Municipal",
        "Privada": " Total_Privada",
        "Pública.1": "Urbana_Pública",
        "Federal.1": "Urbana_Federal",
        "Estadual.1": "Urbana_Estadual",
        "Municipal.1": "Urbana_Municipal",
        "Privada.1": "Urbana_Privada",
        "Pública.2": "Rural_Pública",
        "Federal.2": "Rural_Federal",
        "Estadual.2": "Rural_Estadual",
        "Municipal.2": "Rural_Municipal",
        "Privada.2": "Rural_Privada",
    },
}


localizacao = {
    "educacao_basica": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Basica"],
        "chave": "2.2",
        "valor": "Educação Básica",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "educacao_infantil": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Infantil"],
        # "chave": "2.6",  # Para o ano de 2010
        "chave": "2.7",  # 2011 em frente
        "valor": "Educação Infantil",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "ensino_infantil_creche": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Infantil - Creche"],
        # "chave": "Creche 2.7",  # Para o ano de 2010
        "chave": "Creche 2.8",  # 2011 em frente
        "valor": "Educação Infantil - Creche",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "educacao_infantil_pre_escola": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educação Infantil - Pré-Escola"
        ],
        # "chave": "Pré-Escola 2.10",  # Para o ano de 2010
        "chave": "Pré-Escola 2.12",  # 2011 em frente
        "valor": "Educação Infantil - Pré-Escola",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "ensino_fundamental": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental"],
        "chave": "2.17",  # 2011 em frente
        # "chave": "2.14",  # Para o ano de 2010
        "valor": "Ensino Fundamental",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "ensino_fundamental_anos_iniciais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Ensino Fundamental - Anos Iniciais"
        ],
        "chave": "Anos Iniciais 2.21",  # 2011 em frente
        # "chave": "Anos Iniciais 2.17",  # Para o ano de 2010
        "valor": "Ensino Fundamental - Anos Iniciais",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "ensino_fundamental_anos_finais": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Ensino Fundamental - Anos Finais"
        ],
        "chave": "Anos Finais 2.25",  # 2011 em frente
        # "chave": "Anos Finais 2.20",  # Para o ano de 2010
        "valor": "Ensino Fundamental - Anos Finais",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "ensino_medio": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Médio"],
        "chave": "Ensino Médio 2.29",  # 2011 em frente
        # "chave": "Ensino Médio 2.23",  # Para o ano de 2010
        "valor": "Ensino Médio",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "educacao_profissional": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educação Profissional"],
        "chave": "2.34",  # 2011 em frente
        # "chave": "2.27",  # Para o ano de 2010
        "valor": "Educação Profissional",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "EJA": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["EJA"],
        "chave": "2.39",  # 2011 em frente
        # "chave": "2.31",  # Para o ano de 2010
        "valor": "EJA",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "educacao_especial_classes_comuns": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educação Especial - Classes Comuns"
        ],
        "chave": "2.45",  # 2011 em frente
        # "chave": "2.36",  # Para o ano de 2010
        "valor": "Educação Especial - Classes Comuns",
        "skiprows": 8,
        "table": "docente_localizacao",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educação Especial - Classes Exclusivas"
        ],
        "chave": "2.51",
        # "chave": "2.50",  # Para o ano de 2011
        # "chave": "2.40",  # Para o ano de 2010
        "valor": "Educação Especial - Classes Exclusivas",
        "skiprows": 8,
        "table": "docente_localizacao",
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
        f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
        f"Sinopse_Estatistica_da_Educação_Basica_{ano}.xlsx",
    )
    df = pd.read_excel(
        path_excel,
        skiprows=skiprows,
        sheet_name=chave,
    )

    sheets_etapa_ensino_serie = {chave: valor}

    df_localizacao = {
        name: pd.read_excel(
            path_excel,
            skiprows=skiprows,
            sheet_name=sheet_name,
        )
        for sheet_name, name in sheets_etapa_ensino_serie.items()
    }

    dataframes = {}
    for table_name, columns in df_localizacao.items():
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

    dfs_localizacao = {
        name: drop_unused_columns(
            df.rename(columns=dicionario, errors="raise")
        )
        for name, df in df_localizacao.items()
    }

    df_localizacao = pd.concat(
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
                    var_name="localizacao",
                    value_name="quantidade_docente",
                )
            )
            .assign(etapa_ensino=etapa_ensino)
            for etapa_ensino, df in dfs_localizacao.items()
        ]
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_localizacao["uf"] = (
        df_localizacao["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})  # type: ignore
    )

    df_localizacao = df_localizacao.rename(
        columns={"uf": "sigla_uf"}, errors="raise"
    )

    df_localizacao["rede"] = df_localizacao["localizacao"].apply(
        lambda v: v.split("_")[-1]
    )

    df_localizacao["localizacao"] = df_localizacao["localizacao"].apply(
        lambda v: v.split("_")[0]
    )
    df_localizacao["localizacao"] = df_localizacao["localizacao"].apply(
        lambda x: x.strip()
    )

    df_localizacao["quantidade_docente"] = df_localizacao[
        "quantidade_docente"
    ].astype(int)

    df_localizacao = df_localizacao[
        [
            "sigla_uf",
            "id_municipio",
            "etapa_ensino",
            "rede",
            "localizacao",
            "quantidade_docente",
        ]
    ]

    print("Particionando dados")
    for sigla_uf, df in df_localizacao.groupby("sigla_uf"):
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
        "educacao_infantil",
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
            table=localizacao[x]["table"],
            ano=2023,
            chave=localizacao[x]["chave"],
            valor=localizacao[x]["valor"],
            dicionario=localizacao[x]["dicionario"],
            skiprows=localizacao[x]["skiprows"],
        )
