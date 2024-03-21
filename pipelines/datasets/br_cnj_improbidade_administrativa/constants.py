# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cnj_improbidade_administrativa project
    """

    HOME_PAGE_TEMPLATE = "https://www.cnj.jus.br/improbidade_adm/consultar_requerido.php?validar=form&rs=pesquisarRequeridoGetTabela&rst=&rsrnd=0&rsargs[]=&rsargs[]=&rsargs[]=&rsargs[]=&rsargs[]=&rsargs[]=&rsargs[]=I&rsargs[]=0&rsargs[]=POSICAO_INICIAL_PAGINACAO_PHP{index}&rsargs[]=QUANTIDADE_REGISTROS_PAGINACAO15"

    CONDENACAO_URL_TEMPLATE = "https://www.cnj.jus.br/improbidade_adm/visualizar_condenacao.php?seq_condenacao={id}"

    PROCESS_URL_TEMPLATE = "https://www.cnj.jus.br/improbidade_adm/visualizar_processo.php?seq_processo={id}"

    PEOPLE_INFO_URL_TEMPLATE = "https://www.cnj.jus.br/improbidade_adm/visualizar_condenacao.php?seq_condenacao={sentence_id}&rs=getDadosParte&rst=&rsrnd=0&rsargs[]={people_id}"
