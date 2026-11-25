"""Constantes de br_bndes_fundos_garantidores. Contexto da fonte no README."""

from enum import Enum


class constants(Enum):
    """Config do dataset e de cada tabela (uma por fundo)."""

    DATASET_ID = "br_bndes_fundos_garantidores"

    CENTRAL_DOWNLOADS_URL = (
        "https://www.bndes.gov.br/wps/portal/site/home/transparencia/"
        "centraldedownloads"
    )
    BNDES_URL = "https://www.bndes.gov.br"
    HEADERS = {"User-Agent": "Mozilla/5.0"}
    TIMEOUT = 60

    # nome estável do item na central; a url do arquivo muda a cada posição
    NOME_CENTRAL = {
        "operacoes_fgi_tradicional": "operacoes-do-fgi-tradicional",
        "operacoes_fgi_peac_2022": "operacoes-do-peac-fgi-2022",
        "operacoes_fg_bndes_sebrae": "operacoes-fundo-garantidor-bndes-sebrae",
        "operacoes_fgi_peac_2020": "operacoes-do-fgi-peac-2020",
        "operacoes_fgi_peac_credito_solidario_rs": "operacoes-do-peac-fgi-cs-rs-2023-2024-set24",
        "operacoes_fgenergia": "operacoes-do-fgenergia",
    }

    # só fundos ativos; segunda-feira, um minuto por fundo
    CHECK_UPDATE_CRON = {
        "operacoes_fgi_tradicional": "40 2 * * 1",
        "operacoes_fgi_peac_2022": "45 2 * * 1",
        "operacoes_fg_bndes_sebrae": "55 2 * * 1",
    }

    # cabeçalho padrão (tradicional, peac 2022, sebrae, fgenergia)
    COLUNAS_FONTE = [
        "nome_agente_financeiro",
        "cnpj_cpf_cliente_mascarado",
        "nome_cliente",
        "porte_cliente",
        "valor_credito",
        "valor_garantido",
        "valor_desembolsado",
        "data_solicitacao_outorga",
        "municipio_investimento",
        "uf_investimento",
        "municipio_sede_cliente",
        "uf_sede_cliente",
    ]

    RENAME = {
        "nome_agente_financeiro": "instituicao_financeira_credenciada",
        "cnpj_cpf_cliente_mascarado": "cpf_cnpj_cliente",
        "valor_credito": "valor_operacao",
        "uf_investimento": "sigla_uf",
        "municipio_investimento": "nome_municipio",
        "uf_sede_cliente": "sigla_uf_sede_cliente",
        "municipio_sede_cliente": "nome_municipio_sede_cliente",
    }

    COLUNAS_VALOR = ["valor_operacao", "valor_garantido", "valor_desembolsado"]

    # staging: nomes de município ficam aqui só para o dbt achar o id_municipio
    COLUNAS_STAGING = [
        "data_solicitacao_outorga",
        "sigla_uf",
        "nome_municipio",
        "sigla_uf_sede_cliente",
        "nome_municipio_sede_cliente",
        "cpf_cnpj_cliente",
        "nome_cliente",
        "porte_cliente",
        "valor_operacao",
        "valor_garantido",
        "valor_desembolsado",
        "instituicao_financeira_credenciada",
    ]
