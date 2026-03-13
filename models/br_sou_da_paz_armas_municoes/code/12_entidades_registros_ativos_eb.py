from models.br_sou_da_paz_armas_municoes.code.constants import constants
from models.br_sou_da_paz_armas_municoes.code.main import (
    capitalize,
    change_columns_name,
    consolidado,
    download_file,
)


def entidades_registros_ativos_eb(
    real_file_id: str, sheet_name: str, url_architecture: str
):

    df = download_file(real_file_id=real_file_id, sheet_name=sheet_name)

    rename, order = change_columns_name(url_architecture=url_architecture)
    df = df.rename(columns=rename)

    df = df[order]

    df = capitalize(df=df)

    df = consolidado(df=df)

    df.to_csv(
        constants.tabelas.value["entidades_registros_ativos_eb"]["save_table"],
        sep=",",
        encoding="utf-8",
        index=False,
    )


if __name__ == "__main__":
    entidades_registros_ativos_eb(
        real_file_id=constants.tabelas.value["entidades_registros_ativos_eb"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["entidades_registros_ativos_eb"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "entidades_registros_ativos_eb"
        ]["url_architecture"],
    )
