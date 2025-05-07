# -*- coding: utf-8 -*-
from clean_functions import *  # noqa: F403


def rename_columns(df):
    name_dict = {
        "Ano": "ano",
        "Mês": "mes",
        "Seção - Sigla": "secao_sigla",
        "Seção - Nome": "secao_nome",
        "II": "imposto_importacao",
        "IE": "imposto_exportacao",
        "IPI": "ipi",
        "IRPF": "irpf",
        "IRPJ": "irpj",
        "IRRF": "irrf",
        "IOF": "iof",
        "ITR": "itr",
        "Cofins": "cofins",
        "Pis/Pasep": "pis_pasep",
        "CSLL": "csll",
        "Cide": "cide_combustiveis",
        "Contribuição Previdenciária": "contribuicao_previdenciaria",
        "CPSSS": "cpsss",
        "Pagamento Unificado": "pagamento_unificado",
        "Outras Receitas Administradas": "outras_receitas_rfb",
        "Receitas Não Administradas": "demais_receitas",
    }

    return df.rename(columns=name_dict)


def change_types(df):
    df["ano"] = df["ano"].astype("int")
    df["mes"] = get_month_number(df["mes"])  # noqa: F405
    df["secao_nome"] = df["secao_nome"].str.title()

    # All remaining columns are monetary values
    for col in df.columns[4:]:
        df[col] = (
            df[col].apply(replace_commas).apply(remove_dots).astype("float")  # noqa: F405
        )

    return df


if __name__ == "__main__":
    df = read_data(file_dir="../input/arrecadacao-cnae.csv")  # noqa: F405
    df = remove_empty_columns(df)  # noqa: F405
    df = remove_empty_rows(df)  # noqa: F405
    df = rename_columns(df)
    df = change_types(df)
    save_data(  # noqa: F405
        df=df,
        file_dir="../output/br_rf_arrecadacao_cnae",
        partition_cols=["ano", "mes"],
    )
