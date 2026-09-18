# pyrefly: ignore [missing-import]
from clean_functions import *  # noqa: F403


def rename_columns(df):
    name_dict = {
        "Ano": "ano",
        "Mês": "mes",
        "Tributo": "tributo",
        "Decêndio": "decendio",
        "Arrecadação Bruta": "arrecadacao_bruta",
        "Retificação": "retificacao",
        "Compensação": "compensacao",
        "Restituição": "restituicao",
        "Outros": "outros",
        "Arrecadação Líquida": "arrecadacao_liquida",
    }

    return df.rename(columns=name_dict)


def change_types(df):
    df["ano"] = df["ano"].astype("int")
    # pyrefly: ignore [unknown-name]
    df["mes"] = get_month_number(df["mes"])  # noqa: F405

    # All remaining columns are monetary values
    for col in df.columns[4:]:
        df[col] = (
            # pyrefly: ignore [unknown-name]
            df[col].apply(replace_commas).apply(remove_dots).astype("float")  # noqa: F405
        )

    return df


if __name__ == "__main__":
    # pyrefly: ignore [unknown-name]
    df = read_data(file_dir="../input/arrecadacao-ir-ipi.csv")  # noqa: F405
    df = remove_empty_rows(df)  # noqa: F405  # pyrefly: ignore [unknown-name]
    df = rename_columns(df)
    df = change_types(df)
    # pyrefly: ignore [unknown-name]
    save_data(  # noqa: F405
        df=df,
        file_dir="../output/br_rf_arrecadacao_ir_ipi",
        partition_cols=["ano", "mes"],
    )
