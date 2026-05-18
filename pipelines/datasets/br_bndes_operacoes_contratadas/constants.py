from enum import Enum
from pathlib import Path


class constants(Enum):
    URL_CENTRAL_DOWNLOADS = "https://www.bndes.gov.br/wps/portal/site/home/transparencia/centraldedownloads"
    URL_OPERACOES_CONTRATADAS = "https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx"

    METADATA_SHEET_NAME = "SITE"
    METADATA_NROWS = 2
    METADATA_SKIPROWS = 0
    METADATA_USECOLS = "A:B"

    DATA_SHEET_NAME = "SITE"
    DATA_SKIPROWS = 4
    DATA_USECOLS = None

    DE_PARA_CNAE_NAME = "DE-PARA CNAE"
    DE_PARA_CNAE_SKIPROWS = 3
    DE_PARA_CNAE_USECOLS = [1, 2, 3, 4, 5, 6]

    ROOT_DIR = Path(__file__).parent.parent.parent.parent
    TMP_DIR = ROOT_DIR / "tmp"
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    DATASET_DIR = TMP_DIR / "br_bndes_operacoes_contratadas"
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR = DATASET_DIR / "input"
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR = DATASET_DIR / "output"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    DATA_RENAMMING_MAPPING = {
        "Cliente": "razao_social_cliente",
        "CNPJ": "cnpj_cliente",
        "Descrição do projeto": "descricao_projeto",
        "UF": "sigla_uf",
        "Município": "nome_municipio",
        "Município - código": "id_municipio",
        "Número do contrato": "id_contrato",
        "Data da contratação": "data_contratacao",
        "Valor contratado  R$": "valor_contratado",
        "Valor desembolsado R$": "valor_desembolsado",
        "Fonte de recurso (desembolsos)": "tipo_fonte_recursos",
        "Custo financeiro": "custo_financeiro",
        "Juros": "taxa_juros",
        "Prazo - carência (meses)": "prazo_carencia",
        "Prazo - amortização (meses)": "prazo_amortizacao",
        "Modalidade de apoio": "modalidade_apoio",
        "Forma de apoio": "forma_apoio",
        "Produto": "produto",
        "Instrumento financeiro": "tipo_instrumento_financeiro",
        "Inovação": "indicador_inovacao",
        "Área operacional": "area_operacional_bndes",
        "Setor CNAE": "setor_cnae_bndes",
        "Subsetor CNAE agrupado": "subsetor_agrupado_cnae_bndes",
        "Subsetor CNAE - código": "codigo_cnae_2",
        "Subsetor CNAE - nome": "descricao_subclasse",
        "Setor BNDES": "setor_bndes",
        "Subsetor BNDES": "subsetor_bndes",
        "Porte do cliente": "porte_cliente",
        "Natureza do cliente": "natureza_cliente",
        "Instituição Financeira Credenciada": "nome_instituicao_financeira_credenciada",
        "CNPJ da instituição financeira credenciada": "cnpj_instituicao_financeira_credenciada",
        "Tipo de garantia": "tipo_garantia",
        "Tipo de excepcionalidade": "tipo_excepcionalidade",
        "Situação do contrato": "situacao_contrato",
    }

    DE_PARA_CNAE_RENAME_MAPPING = {
        "Setor  CNAE\n(classificação BNDES)": "setor_cnae_bndes",
        "Subsetor CNAE Agrupado (classificação BNDES)": "subsetor_agrupado_cnae_bndes",
        "Subsetor BNDES": "subsetor_bndes",
        "Setor BNDES": "setor_bndes",
        "Código CNAE - IBGE": "lista_cnaes_2",
        "Produto BNDES": "produto",
    }
