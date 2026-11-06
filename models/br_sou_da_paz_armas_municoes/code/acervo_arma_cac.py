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
)


def acervo_arma_cac(real_file_id: str, sheet_name: str, url_architecture: str):

    df = download_file(real_file_id=real_file_id, sheet_name=sheet_name)

    rename, order = change_columns_name(url_architecture=url_architecture)
    df = df.rename(columns=rename)

    df = df[order]

    df = capitalize(df=df)

    df = consolidado(df=df)

    df = column_br(df=df)

    df = fix_quant(df=df)

    create_output()

    df.to_csv(
        constants.tabelas.value["acervo_arma_cac"]["save_table"],
        sep=",",
        encoding="utf-8",
        index=False,
    )


if __name__ == "__main__":
    acervo_arma_cac(
        real_file_id=constants.tabelas.value["acervo_arma_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["acervo_arma_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["acervo_arma_cac"][
            "url_architecture"
        ],
    )
    tb = bd.Table(
        dataset_id="br_sou_da_paz_armas_municoes",
        table_id="acervo_arma_cac",
        bucket_name="basedosdados-consultoria",
        mode="sou_da_paz",
    )

    tb.create(
        path=constants.tabelas.value["acervo_arma_cac"]["save_table"],
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="csv",
        dataset_is_public=False,
    )
