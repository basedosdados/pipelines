# -*- coding: utf-8 -*-
import pandas as pd
from dictionaries import temas, ufs


def addition_df(xls, iloc_slice: list, table_n: int, abas: list, tema: str):
    temp_df = pd.read_excel(xls, f"T{table_n}", header=7)
    temp_df = temp_df.dropna(thresh=2)
    if len(iloc_slice) > 0:
        temp_df = temp_df.iloc[:, iloc_slice]

    real_df = pd.concat(
        [
            create_model_dataframe(temp_df, n, tema, column)
            for n, column in enumerate(abas)
        ]
    )
    return real_df


def create_model_dataframe(
    temp_df, n: int, tema: str, column: str, ano: int = 2021
):
    columns = ["ano", "uf", "tema", "item", "quantidade_escola"]
    model_df = pd.DataFrame(columns=columns)
    model_df["uf"] = temp_df.iloc[:, 0]
    model_df["tema"] = tema
    model_df["ano"] = ano
    model_df["item"] = column
    model_df["quantidade_escola"] = temp_df.iloc[:, n + 1]

    return model_df


def create_temp_dadataframe(xls, number_table: int, tema: str):
    if number_table == 95:
        temp_df = addition_df(
            xls,
            [0, 1, 3, 5],
            number_table,
            ["Sim", "Não", "Sem Resposta"],
            tema,
        )

    elif 95 < number_table < 109:
        temp_df = addition_df(
            xls,
            [0, 1, 3, 5, 7],
            number_table,
            ["Nunca", "Poucas vezes", "Várias vezes", "Sem resposta"],
            tema,
        )

    elif 109 <= number_table <= 110:
        temp_df = addition_df(
            xls,
            [0, 1, 3, 5, 7, 9],
            number_table,
            [
                "Muito adequado",
                "Adequado",
                "Inadequado",
                "Muito inadequado",
                "Sem resposta",
            ],
            tema,
        )

    elif number_table == 111:
        temp_df = addition_df(
            xls,
            [],
            number_table,
            [
                "Violência",
                "Bullying",
                "Machismo",
                "Homofobia",
                "Uso de drogas",
                "Relações étnico-raciais/racismo",
            ],
            tema,
        )
    else:
        raise ValueError(
            "number_table fora do permitido. Apenas entre 95 á 111"
        )

    return temp_df


def get_clean_data() -> None:
    xls = pd.ExcelFile("../input/anuario-2023.xlsx")

    df = pd.concat(
        [
            create_temp_dadataframe(xls, number_table, tema)
            for tema, number_table in temas.items()
        ]
    )
    df = df.replace(ufs.keys(), ufs.values())

    df = df.drop(df[df["uf"] == "Brasil"].index)

    df["tema"] = df["tema"].str.replace(
        r"% de preenchimento - Temáticas",
        "Proporção de escolas com projeto no tema",
    )

    df.to_csv("../output/br_fbsp_absp_escola_2021.csv", index=False)


if __name__ == "__main__":
    get_clean_data()
