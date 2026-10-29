"""
Constantes de br_stn_capag.
"""

from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly

MUNICIPIOS_TABLE_ID = "municipios"
ESTADOS_TABLE_ID = "estados"
BASE_CALCULO_TABLE_ID = "municipios_base_calculo"

DATASET_ID = "br_stn_capag"

PACKAGE_SHOW_URL = (
    "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show"
)

CKAN_PACKAGES = {
    MUNICIPIOS_TABLE_ID: "capag-municipios",
    ESTADOS_TABLE_ID: "capag-estados",
    # Mesmo pacote de `municipios`: os valores-base vêm da aba Datalake.
    BASE_CALCULO_TABLE_ID: "capag-municipios",
}

DATA_FORMATS = ("CSV", "XLSX")  # Formatos aceitos

HTTP_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 5 * 60
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

# O servidor responde 429 a downloads em sequência: espera entre arquivos
# e tentativas com espera crescente (ou o `Retry-After`, se vier).
DOWNLOAD_PAUSE_SECONDS = 2
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BACKOFF_SECONDS = 10
RETRY_STATUS_CODES = (429, 502, 503, 504)


HEADER_SCAN_ROWS = 15  # Quantas linhas varrer atrás do cabeçalho
PREFERRED_SHEETS = (
    "previa_da_capag",
)  # Abas lidas primeiro, pelo nome normalizado


NULL_TOKENS = ("", "-", "n.d.", "n.d", "nd", "n.e.", "n.e", "ne")

# Colunas correspondentes a indicadores
_INDICADORES = {
    "indicador_1": "indicador_endividamento",
    "nota_1": "nota_endividamento",
    "indicador_2": "indicador_poupanca_corrente",
    "nota_2": "nota_poupanca_corrente",
    "indicador_3": "indicador_liquidez",
    "nota_3": "nota_liquidez",
    "indicador_1_revisao": "indicador_endividamento_revisao",
    "nota_1_revisao": "nota_endividamento_revisao",
    "indicador_2_revisao": "indicador_poupanca_corrente_revisao",
    "nota_2_revisao": "nota_poupanca_corrente_revisao",
    "indicador_3_revisao": "indicador_liquidez_revisao",
    "nota_3_revisao": "nota_liquidez_revisao",
    "capag_revisao": "nota_capag_revisao",
    "capag": "nota_capag",
    "capag_2022": "nota_capag",
    "capag_oficial": "nota_capag",
    "classificacao_capag": "nota_capag",
    "classificacao_da_capag": "nota_capag",
}

COLUNAS_FONTE = {
    MUNICIPIOS_TABLE_ID: {
        "cod_ibge": "id_municipio",
        "codigo_municipio_completo": "id_municipio",
        "uf": "sigla_uf",
        "populacao": "populacao",
        # 2022 revisão e 2023: `Ano_Base`; 2024 em diante: `Origem da Nota Final`
        "ano_base": "ano_base",
        "origem_da_nota_final": "ano_base",
        **_INDICADORES,
    },
    ESTADOS_TABLE_ID: {"uf": "sigla_uf", **_INDICADORES},
}

# Colunas que o cabeçalho precisa ter.
COLUNAS_OBRIGATORIAS = {
    MUNICIPIOS_TABLE_ID: ("id_municipio", "sigla_uf", "nota_capag"),
    ESTADOS_TABLE_ID: ("sigla_uf", "nota_capag"),
}

# Só municípios 2022 traz colunas de revisão; nos demais arquivos ficam nulas.
COLUNAS_REVISAO = [
    "indicador_endividamento_revisao",
    "nota_endividamento_revisao",
    "indicador_poupanca_corrente_revisao",
    "nota_poupanca_corrente_revisao",
    "indicador_liquidez_revisao",
    "nota_liquidez_revisao",
    "nota_capag_revisao",
]

COLUNAS_SAIDA = {
    MUNICIPIOS_TABLE_ID: (
        "id_municipio",
        "sigla_uf",
        "populacao",
        "indicador_endividamento",
        "nota_endividamento",
        "indicador_poupanca_corrente",
        "nota_poupanca_corrente",
        "indicador_liquidez",
        "nota_liquidez",
        "nota_capag",
        "ano_base",
        *COLUNAS_REVISAO,
        "data_referencia",
    ),
    ESTADOS_TABLE_ID: (
        "sigla_uf",
        "indicador_endividamento",
        "nota_endividamento",
        "indicador_poupanca_corrente",
        "nota_poupanca_corrente",
        "indicador_liquidez",
        "nota_liquidez",
        "nota_capag",
        "data_referencia",
    ),
}

COLUNAS_NUMERICAS = (
    "indicador_endividamento",
    "indicador_poupanca_corrente",
    "indicador_liquidez",
    "indicador_endividamento_revisao",
    "indicador_poupanca_corrente_revisao",
    "indicador_liquidez_revisao",
)

# Mínimo de linhas por recurso
MIN_ROWS = {MUNICIPIOS_TABLE_ID: 5000, ESTADOS_TABLE_ID: 26}
CHAVE_UNICA = {
    MUNICIPIOS_TABLE_ID: ("id_municipio",),
    ESTADOS_TABLE_ID: ("sigla_uf",),
}

# Aba Datalake dos arquivos de municípios
DATALAKE_SHEET = "datalake"
DATALAKE_ID_COLUMN = "id_ente"

# Colunas da aba Datalake que não são da DCA, pelo prefixo (depois do ano) do
# cabeçalho: `(demonstrativo, conta, coluna)`. `coluna` é o texto da coluna do
# demonstrativo, que vem colado ao prefixo e antes do detalhe da linha.
CONTAS_DATALAKE = {
    "DemaisObrigacoesFinanceiras": (
        "rgf",
        "Demais obrigações financeiras",
        "Demais Obrigações Financeiras (e)",
    ),
    "DespesasCorrentes": (
        "rreo",
        "Despesas correntes empenhadas até o bimestre",
        "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)",
    ),
    "DespesasCorrentesIntra": (
        "rreo",
        "Despesas correntes intraorçamentárias empenhadas até o bimestre",
        "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)",
    ),
    "DisponibilidadeDeCaixaBruta": (
        "rgf",
        "Disponibilidade de caixa bruta",
        "DISPONIBILIDADE DE CAIXA BRUTA (a)",
    ),
    "DisponibilidadeDeCaixaLiquida": (
        "rgf",
        "Disponibilidade de caixa líquida",
        "DISPONIBILIDADE DE CAIXA LÍQUIDA (ANTES DA INSCRIÇÃO EM RESTOS A "
        "PAGAR NÃO PROCESSADOS DO EXERCÍCIO) (g)=(a-(b+c+d+e)-f)",
    ),
    "DividaConsolidada": ("rgf", "Dívida consolidada", "Último Período"),
    "RGF2ReceitaCorrenteLiquida": (
        "rgf",
        "Receita corrente líquida",
        "Último Período",
    ),
    "ReceitasCorrentes": (
        "rreo",
        "Receitas correntes realizadas até o bimestre",
        "Até o Bimestre (c)",
    ),
    "ReceitasCorrentesIntra": (
        "rreo",
        "Receitas correntes intraorçamentárias realizadas até o bimestre",
        "Até o Bimestre (c)",
    ),
    "RestosAPagarEmpenhadosENaoLiquidadosDeExerciciosAnteriores": (
        "rgf",
        "Restos a pagar empenhados e não liquidados de exercícios anteriores",
        "Restos a Pagar Empenhados e Não Liquidados de Exercícios "
        "Anteriores (d)",
    ),
    "RestosAPagarLiquidadosENaoPagosDeExerciciosAnteriores": (
        "rgf",
        "Restos a pagar liquidados e não pagos de exercícios anteriores",
        "De Exercícios Anteriores (b)",
    ),
    "RestosAPagarLiquidadosENaoPagosDoExercicio": (
        "rgf",
        "Restos a pagar liquidados e não pagos do exercício",
        "Do Exercício (c)",
    ),
}

COLUNAS_SAIDA_BASE_CALCULO = (
    "id_municipio",
    "ano_base",
    "demonstrativo",
    "tipo",
    "codigo_conta",
    "conta",
    "detalhe",
    "valor",
    "data_referencia",
)
CHAVE_UNICA_BASE_CALCULO = (
    "id_municipio",
    "ano_base",
    "demonstrativo",
    "tipo",
    "codigo_conta",
    "conta",
    "detalhe",
)

COVERAGE = AllFree(
    date_column=DateOnly(col="data_referencia"),
    date_format=DateFormat.YEAR_MD,
)
