# -*- coding: utf-8 -*-
from clean_functions import *  # noqa: F403


def rename_columns(df):
    name_dict = {
        "Ano": "ano",
        "Mês": "mes",
        "Unidade da Federação": "nome_uf",
        "Região Política": "regiao_politica",
        "Cidade e UF": "cidade_uf",
        "Valor": "valor_arrecadado",
    }

    return df.rename(columns=name_dict)


def change_types(df):
    df["ano"] = df["ano"].astype("int")
    df["mes"] = get_month_number(df["mes"])  # noqa: F405
    df["valor_arrecadado"] = (
        df["valor_arrecadado"]
        .apply(replace_commas)  # noqa: F405
        .apply(remove_dots)  # noqa: F405
        .astype("float")
    )

    return df


def format_state(df):
    df["sigla_uf"] = get_state_letters(df["nome_uf"])  # noqa: F405
    return df.drop("nome_uf", axis=1)


def format_region(df):
    df["sigla_regiao"] = get_region_letters(df["regiao_politica"])  # noqa: F405
    return df.drop("regiao_politica", axis=1)


def format_city(df):
    df["cidade"] = df["cidade_uf"].str.split(" - ").str[0]
    return df.drop("cidade_uf", axis=1)


if __name__ == "__main__":
    df = read_data(file_dir="../input/arrecadacao-itr.csv")  # noqa: F405
    df = remove_empty_rows(df)  # noqa: F405
    df = rename_columns(df)
    df = change_types(df)
    df = format_state(df)
    df = format_region(df)
    df = format_city(df)
    save_data(  # noqa: F405
        df=df,
        file_dir="../output/br_rf_arrecadacao_itr",
        partition_cols=["ano", "mes"],
    )
