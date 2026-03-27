import models.br_sou_da_paz_armas_municoes.code.acervo_arma_cac as acervo_arma_cac
import models.br_sou_da_paz_armas_municoes.code.acervo_arma_outras_categorias_exercito_brasileiro as acervo_arma_outras_categorias_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.destruicao_exercito_brasileiro as destruicao_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.municao_vendida as municao_vendida
import models.br_sou_da_paz_armas_municoes.code.nova_arma_cac as nova_arma_cac
import models.br_sou_da_paz_armas_municoes.code.nova_arma_outras_categorias_exercito_brasileiro as nova_arma_outras_categorias_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.nova_arma_registro_cac as nova_arma_registro_cac
import models.br_sou_da_paz_armas_municoes.code.nova_entidade_exercito_brasileiro as nova_entidade_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.nova_loja_exercito_brasileiro as nova_loja_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.novo_registro_policia_federal as novo_registro_policia_federal
import models.br_sou_da_paz_armas_municoes.code.pessoa_fisica_cac as pessoa_fisica_cac
import models.br_sou_da_paz_armas_municoes.code.pessoa_fisica_outras_categorias_exercito_brasileiro as pessoa_fisica_outras_categorias_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.pessoa_fisica_policia_federal as pessoa_fisica_policia_federal
import models.br_sou_da_paz_armas_municoes.code.registro_ativo_cac as registro_ativo_cac
import models.br_sou_da_paz_armas_municoes.code.registro_ativo_entidade_exercito_brasileiro as registro_ativo_entidade_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.registro_ativo_loja_exercito_brasileiro as registro_ativo_loja_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.registro_ativo_policia_federal as registro_ativo_policia_federal
import models.br_sou_da_paz_armas_municoes.code.registro_emitido_policia_federal as registro_emitido_policia_federal
import models.br_sou_da_paz_armas_municoes.code.registro_vencido_exercito_brasileiro as registro_vencido_exercito_brasileiro
import models.br_sou_da_paz_armas_municoes.code.visita_fiscalizacao_exercito_brasileiro as visita_fiscalizacao_exercito_brasileiro
from models.br_sou_da_paz_armas_municoes.code.constants import constants

if __name__ == "__main__":
    acervo_arma_cac.acervo_arma_cac(
        real_file_id=constants.tabelas.value["acervo_arma_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["acervo_arma_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["acervo_arma_cac"][
            "url_architecture"
        ],
    )

    acervo_arma_outras_categorias_exercito_brasileiro.acervo_arma_outras_categorias_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "acervo_arma_outras_categorias_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "acervo_arma_outras_categorias_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "acervo_arma_outras_categorias_exercito_brasileiro"
        ]["url_architecture"],
    )

    destruicao_exercito_brasileiro.destruicao_exercito_brasileiro(
        real_file_id=constants.tabelas.value["destruicao_exercito_brasileiro"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["destruicao_exercito_brasileiro"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "destruicao_exercito_brasileiro"
        ]["url_architecture"],
    )

    nova_arma_outras_categorias_exercito_brasileiro.nova_arma_outras_categorias_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "nova_arma_outras_categorias_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "nova_arma_outras_categorias_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "nova_arma_outras_categorias_exercito_brasileiro"
        ]["url_architecture"],
    )

    nova_entidade_exercito_brasileiro.nova_entidade_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "nova_entidade_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "nova_entidade_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "nova_entidade_exercito_brasileiro"
        ]["url_architecture"],
    )

    registro_ativo_entidade_exercito_brasileiro.registro_ativo_entidade_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "registro_ativo_entidade_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "registro_ativo_entidade_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "registro_ativo_entidade_exercito_brasileiro"
        ]["url_architecture"],
    )

    nova_loja_exercito_brasileiro.nova_loja_exercito_brasileiro(
        real_file_id=constants.tabelas.value["nova_loja_exercito_brasileiro"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["nova_loja_exercito_brasileiro"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "nova_loja_exercito_brasileiro"
        ]["url_architecture"],
    )

    registro_ativo_loja_exercito_brasileiro.registro_ativo_loja_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "registro_ativo_loja_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "registro_ativo_loja_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "registro_ativo_loja_exercito_brasileiro"
        ]["url_architecture"],
    )

    municao_vendida.municao_vendida(
        real_file_id=constants.tabelas.value["municao_vendida"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["municao_vendida"]["sheet_name"],
        url_architecture=constants.tabelas.value["municao_vendida"][
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

    pessoa_fisica_outras_categorias_exercito_brasileiro.pessoa_fisica_outras_categorias_exercito_brasileiro(
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

    registro_ativo_cac.registro_ativo_cac(
        real_file_id=constants.tabelas.value["registro_ativo_cac"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["registro_ativo_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["registro_ativo_cac"][
            "url_architecture"
        ],
    )

    registro_ativo_policia_federal.registro_ativo_policia_federal(
        real_file_id=constants.tabelas.value["registro_ativo_policia_federal"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["registro_ativo_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "registro_ativo_policia_federal"
        ]["url_architecture"],
    )

    nova_arma_registro_cac.nova_arma_registro_cac(
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

    novo_registro_policia_federal.novo_registro_policia_federal(
        real_file_id=constants.tabelas.value["novo_registro_policia_federal"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["novo_registro_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "novo_registro_policia_federal"
        ]["url_architecture"],
    )

    nova_arma_cac.nova_arma_cac(
        real_file_id=constants.tabelas.value["nova_arma_cac"]["real_file_id"],
        sheet_name=constants.tabelas.value["nova_arma_cac"]["sheet_name"],
        url_architecture=constants.tabelas.value["nova_arma_cac"][
            "url_architecture"
        ],
    )

    pessoa_fisica_policia_federal.pessoa_fisica_policia_federal(
        real_file_id=constants.tabelas.value["pessoa_fisica_policia_federal"][
            "real_file_id"
        ],
        sheet_name=constants.tabelas.value["pessoa_fisica_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "pessoa_fisica_policia_federal"
        ]["url_architecture"],
    )

    registro_emitido_policia_federal.registro_emitido_policia_federal(
        real_file_id=constants.tabelas.value[
            "registro_emitido_policia_federal"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value["registro_emitido_policia_federal"][
            "sheet_name"
        ],
        url_architecture=constants.tabelas.value[
            "registro_emitido_policia_federal"
        ]["url_architecture"],
    )

    registro_vencido_exercito_brasileiro.registro_vencido_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "registro_vencido_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "registro_vencido_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "registro_vencido_exercito_brasileiro"
        ]["url_architecture"],
    )

    visita_fiscalizacao_exercito_brasileiro.visita_fiscalizacao_exercito_brasileiro(
        real_file_id=constants.tabelas.value[
            "visita_fiscalizacao_exercito_brasileiro"
        ]["real_file_id"],
        sheet_name=constants.tabelas.value[
            "visita_fiscalizacao_exercito_brasileiro"
        ]["sheet_name"],
        url_architecture=constants.tabelas.value[
            "visita_fiscalizacao_exercito_brasileiro"
        ]["url_architecture"],
    )
