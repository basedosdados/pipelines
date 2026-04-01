import basedosdados as bd

from models.br_sou_da_paz_armas_municoes.code.constants import constants
from models.br_sou_da_paz_armas_municoes.code.tasks import (
    capitalize,
    change_columns_name,
    column_br,
    consolidado,
    create_output,
    download_file,
    fix_quant,
    where_not_null,
)


def pessoa_fisica_outras_categorias_exercito_brasileiro(
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
        constants.tabelas.value[
            "pessoa_fisica_outras_categorias_exercito_brasileiro"
        ]["save_table"],
        sep=",",
        encoding="utf-8",
        index=False,
    )


if __name__ == "__main__":
    pessoa_fisica_outras_categorias_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "pessoa_fisica_outras_categorias_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "pessoa_fisica_outras_categorias_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "pessoa_fisica_outras_categorias_exercito_brasileiro"
        ]["url_architecture"],
    )
    tb = bd.Table(
        dataset_id="br_sou_da_paz_armas_municoes",
        table_id="pessoa_fisica_outras_categorias_exercito_brasileiro",
    )

    tb.create(
        path=constants.tabelas.value[
            "pessoa_fisica_outras_categorias_exercito_brasileiro"
        ]["save_table"],
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="csv",
        dataset_is_public=False,
        folder="sou_da_paz",
    )
