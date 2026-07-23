"""
Constantes do dataset br_ibge_pnadc (dicionário).
"""

URL_DOCUMENTACAO = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Trimestral/Microdados/Documentacao"
)

ARQUIVO_ZIP_DICIONARIO = "Dicionario_e_input_20221031.zip"
ARQUIVO_DICIONARIO_NO_ZIP = "dicionario_PNADC_microdados_trimestral.xls"
ARQUIVO_OCUPACAO = "Estrutura_Ocupacao_COD.xls"

#: Colunas do bloco de escolaridade. O Excel do IBGE as lista apenas sob a Parte
#: de educação, mas a tabela `microdados` também as contém — a mesma
#: decodificação serve as duas.
COLUNAS_V3_EM_MICRODADOS = [
    "V3001",
    "V3002",
    "V3002A",
    "V3003",
    "V3003A",
    "V3004",
    "V3005",
    "V3005A",
    "V3006",
    "V3006A",
    "V3007",
    "V3008",
    "V3009",
    "V3009A",
    "V3010",
    "V3011",
    "V3011A",
    "V3012",
    "V3013",
    "V3013A",
    "V3013B",
    "V3014",
]

#: Colunas codificadas cuja `chave` vem com zero à esquerda no dicionário
#: (`01`-`09`) mas com dígito único no dado (`1`-`9`).
#: V4010 NÃO entra aqui: é código hierárquico onde o zero é semântico
#: (`0110` != `110`) — ver issue #1699.
COLUNAS_STRIP_ZERO = [
    "V2005",
    "V4072",
    "V4074A",
    "V3003",
    "V3003A",
    "V3006",
    "V3009",
    "V3009A",
    "V3013",
]

#: Ordem final das colunas do dicionário (contrato com o staging/dbt).
COLUNAS_DICIONARIO = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]
