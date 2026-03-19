import models.br_sou_da_paz_armas_municoes.code.armas_acervo_cac as armas_acervo_cac
import models.br_sou_da_paz_armas_municoes.code.armas_acervo_outras_categorias as armas_acervo_outras_categorias
import models.br_sou_da_paz_armas_municoes.code.armas_destruidas as armas_destruidas
import models.br_sou_da_paz_armas_municoes.code.armas_novas_outras_categorias as armas_novas_outras_categorias
import models.br_sou_da_paz_armas_municoes.code.entidades_novas as entidades_novas
import models.br_sou_da_paz_armas_municoes.code.entidades_registro_ativo as entidades_registro_ativo
import models.br_sou_da_paz_armas_municoes.code.lojas_novas as lojas_novas
import models.br_sou_da_paz_armas_municoes.code.lojas_registro_ativo as lojas_registro_ativo
import models.br_sou_da_paz_armas_municoes.code.municoes as municoes
import models.br_sou_da_paz_armas_municoes.code.pessoa_fisica_cac as pessoa_fisica_cac
import models.br_sou_da_paz_armas_municoes.code.pessoa_fisica_outras_categorais as pessoa_fisica_outras_categorais
import models.br_sou_da_paz_armas_municoes.code.registros_ativos_cac as registros_ativos_cac
import models.br_sou_da_paz_armas_municoes.code.registros_ativos_policia_federal as registros_ativos_policia_federal
import models.br_sou_da_paz_armas_municoes.code.registros_novos_cac as registros_novos_cac
import models.br_sou_da_paz_armas_municoes.code.registros_novos_policia_federal as registros_novos_policia_federal
from models.br_sou_da_paz_armas_municoes.code.constants import constants

if __name__ == "__main__":
    armas_acervo_cac.armas_acervo_cac(
        real_file_id=constants.tabelas.value["armas_acervo_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["armas_acervo_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["armas_acervo_cac"][
            "url_architecture"
        ],
    )

    armas_acervo_outras_categorias.armas_acervo_outras_categorias(
        real_file_id=constants.tabelas.value["armas_acervo_outras_categorias"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["armas_acervo_outras_categorias"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "armas_acervo_outras_categorias"
        ]["url_architecture"],
    )

    armas_destruidas.armas_destruidas(
        real_file_id=constants.tabelas.value["armas_destruidas"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["armas_destruidas"]["sheet_name"],
        url_architecture=constants.tabelas.value["armas_destruidas"][
            "url_architecture"
        ],
    )

    armas_novas_outras_categorias.armas_novas_outras_categorias(
        real_file_id=constants.tabelas.value["armas_novas_outras_categorias"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["armas_novas_outras_categorias"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "armas_novas_outras_categorias"
        ]["url_architecture"],
    )

    entidades_novas.entidades_novas(
        real_file_id=constants.tabelas.value["entidades_novas"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["entidades_novas"]["sheet_name"],
        url_architecture=constants.tabelas.value["entidades_novas"][
            "url_architecture"
        ],
    )

    entidades_registro_ativo.entidades_registro_ativo(
        real_file_id=constants.tabelas.value["entidades_registro_ativo"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["entidades_registro_ativo"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value["entidades_registro_ativo"][
            "url_architecture"
        ],
    )

    lojas_novas.lojas_novas(
        real_file_id=constants.tabelas.value["lojas_novas"]["real_file_id"],
        sheet_name=constants.tabelas.value["lojas_novas"]["sheet_name"],
        url_architecture=constants.tabelas.value["lojas_novas"][
            "url_architecture"
        ],
    )

    lojas_registro_ativo.lojas_registro_ativo(
        real_file_id=constants.tabelas.value["lojas_registro_ativo"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["lojas_registro_ativo"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value["lojas_registro_ativo"][
            "url_architecture"
        ],
    )

    municoes.municoes(
        real_file_id=constants.tabelas.value["municoes"]["real_file_id"],
        sheet_name=constants.tabelas.value["municoes"]["sheet_name"],
        url_architecture=constants.tabelas.value["municoes"][
            "url_architecture"
        ],
    )

    pessoa_fisica_cac.pessoa_fisica_cac(
        real_file_id=constants.tabelas.value["pessoa_fisica_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["pessoa_fisica_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["pessoa_fisica_cac"][
            "url_architecture"
        ],
    )

    pessoa_fisica_outras_categorais.pessoa_fisica_outras_categorais(
        real_file_id=constants.tabelas.value[
            "pessoa_fisica_outras_categorais"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value["pessoa_fisica_outras_categorais"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "pessoa_fisica_outras_categorais"
        ]["url_architecture"],
    )

    registros_ativos_cac.registros_ativos_cac(
        real_file_id=constants.tabelas.value["registros_ativos_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["registros_ativos_cac"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value["registros_ativos_cac"][
            "url_architecture"
        ],
    )

    registros_ativos_policia_federal.registros_ativos_policia_federal(
        real_file_id=constants.tabelas.value[
            "registros_ativos_policia_federal"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value["registros_ativos_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "registros_ativos_policia_federal"
        ]["url_architecture"],
    )

    registros_novos_cac.registros_novos_cac(
        real_file_id=constants.tabelas.value["registros_novos_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["registros_novos_cac"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value["registros_novos_cac"][
            "url_architecture"
        ],
    )

    registros_novos_policia_federal.registros_novos_policia_federal(
        real_file_id=constants.tabelas.value[
            "registros_novos_policia_federal"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value["registros_novos_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "registros_novos_policia_federal"
        ]["url_architecture"],
    )
