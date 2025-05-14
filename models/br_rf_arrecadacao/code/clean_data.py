# -*- coding: utf-8 -*-
import os

import pandas as pd

file_directory = os.path.dirname(__file__)


def read_data():
    data_directory = os.path.join(
        file_directory, "../input/arrecadacao-estado.csv"
    )
    return pd.read_csv(data_directory, sep=";")


def removeEmptyRows(df):
    return df.dropna(axis=0, how="all")


def rename_columns(df):
    name_dict = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "IMPOSTO SOBRE IMPORTAÇÃO": "imposto_importacao",
        "IMPOSTO SOBRE EXPORTAÇÃO": "imposto_exportacao",
        "IPI - FUMO": "ipi_fumo",
        "IPI - BEBIDAS": "ipi_bebidas",
        "IPI - AUTOMÓVEIS": "ipi_automoveis",
        "IPI - VINCULADO À IMPORTACAO": "ipi_importacoes",
        "IPI - OUTROS": "ipi_outros",
        "IRPF": "irpf",
        "IRPJ - ENTIDADES FINANCEIRAS": "irpj_entidades_financeiras",
        "IRPJ - DEMAIS EMPRESAS": "irpj_demais_empresas",
        "IRRF - RENDIMENTOS DO TRABALHO": "irrf_rendimentos_trabalho",
        "IRRF - RENDIMENTOS DO CAPITAL": "irrf_rendimentos_capital",
        "IRRF - REMESSAS P/ EXTERIOR": "irrf_remessas_exterior",
        "IRRF - OUTROS RENDIMENTOS": "irrf_outros_rendimentos",
        "IMPOSTO S/ OPERAÇÕES FINANCEIRAS": "iof",
        "IMPOSTO TERRITORIAL RURAL": "itr",
        "IMPOSTO PROVIS.S/ MOVIMENT. FINANC. - IPMF": "ipmf",
        "CPMF": "cpmf",
        "COFINS": "cofins",
        "COFINS - FINANCEIRAS": "cofins_financeiras",
        "COFINS - DEMAIS": "cofins_demais_empresas",
        "CONTRIBUIÇÃO PARA O PIS/PASEP": "pis_pasep",
        "CONTRIBUIÇÃO PARA O PIS/PASEP - FINANCEIRAS": "pis_pasep_entidades_financeiras",
        "CONTRIBUIÇÃO PARA O PIS/PASEP - DEMAIS": "pis_pasep_demais_empresas",
        "CSLL": "csll",
        "CSLL - FINANCEIRAS": "csll_financeiras",
        "CSLL - DEMAIS": "csll_demais_empresas",
        "CIDE-COMBUSTÍVEIS (parc. não dedutível)": "cide_combustiveis_parcela_nao_dedutivel",
        "CIDE-COMBUSTÍVEIS": "cide_combustiveis",
        "CONTRIBUIÇÃO PLANO SEG. SOC. SERVIDORES": "cpsss_1",
        "CPSSS - Contrib. p/ o Plano de Segurid. Social Serv. Público": "cpsss_2",
        "CONTRIBUICÕES PARA FUNDAF": "contribuicoes_fundaf",
        "REFIS": "refis",
        "PAES": "paes",
        "RETENÇÃO NA FONTE - LEI 10.833, Art. 30": "retencoes_fonte",
        "PAGAMENTO UNIFICADO": "pagamento_unificado",
        "OUTRAS RECEITAS ADMINISTRADAS": "outras_receitas_",
        "DEMAIS RECEITAS": "demais_receitas",
        "RECEITA PREVIDENCIÁRIA": "receita_previdenciaria",
        "RECEITA PREVIDENCIÁRIA - PRÓPRIA": "receita_previdenciaria_propria",
        "RECEITA PREVIDENCIÁRIA - DEMAIS": "receita_previdenciaria_demais",
        "ADMINISTRADAS POR OUTROS ÓRGÃOS": "receitas_outros_orgaos",
    }

    return df.rename(columns=name_dict)


def replace_commas(value):
    string_value = str(value)
    num_commas = string_value.count(",")
    if num_commas == 1:
        return string_value.replace(",", ".")
    elif num_commas > 1:
        return string_value.replace(",", "", num_commas - 1).replace(",", ".")
    else:
        return string_value


def remove_dots(value):
    string_value = str(value)
    num_dots = string_value.count(".")
    if num_dots > 1:
        return string_value.replace(".", "", num_dots - 1)
    else:
        return string_value


def get_month_number(month_column):
    month_numbers = {
        "Janeiro": "1",
        "Fevereiro": "2",
        "Março": "3",
        "Abril": "4",
        "Maio": "5",
        "Junho": "6",
        "Julho": "7",
        "Agosto": "8",
        "Setembro": "9",
        "Outubro": "10",
        "Novembro": "11",
        "Dezembro": "12",
    }
    return month_column.replace(month_numbers).astype("int")


def change_types(df):
    df["ano"] = df["ano"].astype("int")
    df["mes"] = get_month_number(df["mes"])
    df["sigla_uf"] = df["sigla_uf"].astype("string")

    # All remaining columns are monetary values
    for col in df.columns[3:]:
        df[col] = (
            df[col].apply(replace_commas).apply(remove_dots).astype("float")
        )

    return df


def save_data(df):
    data_directory = os.path.join(
        file_directory, "../output/br_rf_arrecadacao"
    )
    df.to_parquet(data_directory, partition_cols=["ano", "mes"])


if __name__ == "__main__":
    df = read_data()
    df = removeEmptyRows(df)
    df = rename_columns(df)
    df = change_types(df)
    save_data(df)
