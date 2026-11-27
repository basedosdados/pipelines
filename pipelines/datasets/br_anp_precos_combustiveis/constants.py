"""
Constant values for br_anp_precos_combustiveis.
"""

from enum import Enum

DATASET_ID = "br_anp_precos_combustiveis"

# Única tabela do dataset.
MICRODADOS_TABLE_ID = "microdados"


class constants(Enum):
    RENAME = {
        "Revenda": "nome_estabelecimento",
        "CNPJ da Revenda": "cnpj_revenda",
        "Bairro": "bairro_revenda",
        "Cep": "cep_revenda",
        "Produto": "produto",
        "Valor de Venda": "preco_venda",
        "Valor de Compra": "preco_compra",
        "Unidade de Medida": "unidade_medida",
        "Bandeira": "bandeira_revenda",
        "Estado - Sigla": "sigla_uf",
        "Municipio": "nome",
        "Data da Coleta": "data_coleta",
        "Nome da Rua": "nome_rua",
        "Numero Rua": "numero_rua",
        "Complemento": "complemento",
    }

    UNIDADES = {
        "R$ / litro": "R$/litro",
        "R$ / m³": "R$/m3",
        # grafia do semestral ca-2025-02
        "R$ / m3": "R$/m3",
        "R$ / 13 kg": "R$/13kg",
    }

    ORDEM = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "bairro_revenda",
        "cep_revenda",
        "endereco_revenda",
        "cnpj_revenda",
        "nome_estabelecimento",
        "bandeira_revenda",
        "data_coleta",
        "produto",
        "unidade_medida",
        "preco_compra",
        "preco_venda",
    ]

    URLS = [
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
    ]

    URLS_DATA = [
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv"
    ]

    PATH_INPUT = "/tmp/input/"

    PATH_OUTPUT = "/tmp/output/"

    URL_DIESEL_GNV = "/tmp/input/ultimas-4-semanas-diesel-gnv.csv"
    URL_GASOLINA_ETANOL = "/tmp/input/ultimas-4-semanas-gasolina-etanol.csv"
    URL_GLP = "/tmp/input/ultimas-4-semanas-glp.csv"

    # tabela de prod olhada pelo verificar_lacuna
    TABELA_PROD = "basedosdados.br_anp_precos_combustiveis.microdados"
    # maior intervalo aceito entre duas datas de coleta seguidas
    DIAS_SEM_COLETA = 7
    # quantos dias para trás o verificar_lacuna olha
    JANELA_LACUNA = 182
    # lacunas que também faltam nos semestrais, como (última data, próxima data)
    LACUNAS_DA_FONTE = []
