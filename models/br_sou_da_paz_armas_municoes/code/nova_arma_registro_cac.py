from models.br_sou_da_paz_armas_municoes.code.constants import constants
from models.br_sou_da_paz_armas_municoes.code.main import (
    capitalize,
    change_columns_name,
    column_br,
    consolidado,
    create_output,
    download_file,
    fix_quant,
    where_not_null,
)


def nova_arma_registro_cac(
    real_file_id: str, sheet_name: str, url_architecture: str
):

    df = download_file(real_file_id=real_file_id, sheet_name=sheet_name)

    rename, order = change_columns_name(url_architecture=url_architecture)
    df = df.rename(columns=rename)

    df = df[order]

    df = capitalize(df=df)

    df = consolidado(df=df)
    df = column_br(df=df)
    df = fix_quant(df=df)
    df = where_not_null(df=df)
    create_output()
    df.to_csv(
        constants.tabelas.value["nova_arma_registro_cac"]["save_table"],
        sep=",",
        encoding="utf-8",
        index=False,
    )


if __name__ == "__main__":
    nova_arma_registro_cac(
        real_file_id=constants.tabelas.value["nova_arma_registro_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["nova_arma_registro_cac"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value["nova_arma_registro_cac"][
            "url_architecture"
        ],
    )
