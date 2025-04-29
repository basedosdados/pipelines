# -*- coding: utf-8 -*-
import os
import pandas as pd
from columns import columns_order, real_columns


def set_row(df):
    keys = df.columns.tolist()
    modelo_dict = {keys[n]: 0 for n in range(len(keys))}
    return modelo_dict


def get_clean_data():

    arquivos = os.listdir(os.getcwd().replace("code", "input"))

    dfs = [pd.read_excel(f"../input/{df}") for df in arquivos]
    df = pd.concat(dfs)

    df_group = df.groupby(["ano", "uf"], group_keys=True).apply(lambda x: x)[["tipo_de_crime", "qtd"]]
    df_raw = pd.DataFrame(columns=columns_order)

    modelo_dict = set_row(df_raw)
    for ano in df["ano"].sort_values(ascending=False).unique():
        for uf in df["uf"].unique():
            condition = (df_group.index.get_level_values("uf") == uf) & (df_group.index.get_level_values("ano") == ano)
            modelo_dict["ano"] = ano
            modelo_dict["sigla_uf"] = uf
            for dado in df_group[condition].iloc:
                modelo_dict[dado[0]] = dado[1]
            df_raw.loc[len(df_raw)] = modelo_dict
            modelo_dict = set_row(df_raw)

    remove_list_colunas = [
        "Mortes decorrentes de intervenções policiais - Total (Policiais civis e militares em serviço e fora)",
        "Roubo e furto de veículos"]

    for remove in remove_list_colunas:
        df_raw = df_raw.drop(remove, axis=1)

    for column in df_raw.columns:
        if column not in ["ano", "sigla_uf", "Despesas empenhadas na Função Segurança Pública"]:
            df_raw[column] = df_raw[column].astype("Int64")

    df_raw.columns = real_columns
    df_raw = df_raw[~df_raw["sigla_uf"].isin(["Brasil"])]

    df_raw.to_csv("../output/br_fbsp_absp_uf.csv", index=False)


if __name__ == '__main__':
    get_clean_data()
