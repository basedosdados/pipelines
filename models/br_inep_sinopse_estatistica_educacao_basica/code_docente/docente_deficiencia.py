import os

import basedosdados as bd
import pandas as pd

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

# os.makedirs(INPUT, exist_ok=True)
# os.makedirs(OUTPUT, exist_ok=True)

RENAME_DEFICIENCIA = {
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
        # "Transtorno do Espectro Autista": "Transtorno do Espectro Autista",
        # "Altas Habilidades / Superdotação": "Altas Habilidades / Superdotação",
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
        # "Transtorno do Espectro Autista": "Transtorno do Espectro Autista",
        # "Altas Habilidades / Superdotação": "Altas Habilidades / Superdotação",
    },
}

deficiencia = {
    "educacao_especial_classes_comuns": {
        "dicionario": RENAME_DEFICIENCIA["Educacao Especial - Classes Comuns"],
        "chave": "2.48",
        "valor": "Educacao Especial - Classes Comuns",
        "skiprows": 7,
        "table": "docente_deficiencia",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAME_DEFICIENCIA[
            "Educacao Especial - Classes Exclusivas"
        ],
        "chave": "2.54",
        "valor": "Educacao Especial - Classes Exclusivas",
        "skiprows": 7,
        "table": "docente_deficiencia",
    },
}


def read_sheet(
    table: str, ano: int, chave: str, valor: str, dicionario: dict, skiprows
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

    sheets_etapa_ensino_serie = {chave: valor}

    dfs_deficiencia = {
        name: pd.read_excel(
            path_excel, skiprows=skiprows, sheet_name=sheet_name
        )
        for sheet_name, name in sheets_etapa_ensino_serie.items()
    }

    dataframes = {}

    for table_name, columns in dfs_deficiencia.items():
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

    dfs_deficiencia = {
        name: drop_unused_columns(
            df.rename(columns=RENAME_DEFICIENCIA[name], errors="raise")
        )
        for name, df in dfs_deficiencia.items()
    }

    df_deficiencia = pd.concat(
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
                    var_name="deficiencia",
                    value_name="quantidade_docente",
                )
            )
            .assign(tipo_classe=tipo_classe)
            for tipo_classe, df in dfs_deficiencia.items()
        ]
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_deficiencia["uf"] = (
        df_deficiencia["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
    )  # type: ignore

    df_deficiencia = df_deficiencia.rename(
        columns={"uf": "sigla_uf"}, errors="raise"
    )

    df_deficiencia["quantidade_docente"] = df_deficiencia[
        "quantidade_docente"
    ].astype(int)

    print("Particionando dados")
    for sigla_uf, df in df_deficiencia.groupby("sigla_uf"):
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
        "educacao_especial_classes_comuns",
        "educacao_especial_classes_exclusivas",
    ]

    for x in lista:
        # for ano in range(2012, 2019):
        read_sheet(
            table=deficiencia[x]["table"],
            ano=2011,
            chave=deficiencia[x]["chave"],
            valor=deficiencia[x]["valor"],
            dicionario=deficiencia[x]["dicionario"],
            skiprows=deficiencia[x]["skiprows"],
        )
