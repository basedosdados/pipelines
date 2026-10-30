"""
Constant values for br_sp_saopaulo_geosampa_iptu.
"""

DATASET_ID = "br_sp_saopaulo_geosampa_iptu"
IPTU_TABLE_ID = "iptu"

# um zip por ano de exercício
URL_TEMPLATE = (
    "https://geosampa.prefeitura.sp.gov.br/PaginasPublicas/downloadArquivo.aspx"
    "?orig=DownloadCamadas&arq=12_Cadastro\\IPTU_INTER\\XLS_CSV\\IPTU_{ano}"
    "&arqTipo=XLS_CSV"
)

# a fonte exige headers de navegador
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) "
        "Gecko/20100101 Firefox/134.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
}

# anos testados antes de desistir da busca
MAX_ANOS_BUSCA = 4

# campos por linha do csv da fonte
N_CAMPOS_FONTE = 29

RENAME = {
    "NUMERO DO CONTRIBUINTE": "numero_contribuinte",
    "ANO DO EXERCICIO": "ano",
    "NUMERO DA NL": "numero_notificacao",
    "DATA DO CADASTRAMENTO": "data_cadastramento",
    "NUMERO DO CONDOMINIO": "numero_condominio",
    "CODLOG DO IMOVEL": "codigo_logradouro",
    "NOME DE LOGRADOURO DO IMOVEL": "logradouro",
    "NUMERO DO IMOVEL": "numero_imovel",
    "COMPLEMENTO DO IMOVEL": "complemento",
    "BAIRRO DO IMOVEL": "bairro",
    "REFERENCIA DO IMOVEL": "referencia_imovel",
    "CEP DO IMOVEL": "cep",
    "TIPO DE USO DO IMOVEL": "finalidade_imovel",
    "TIPO DE PADRAO DA CONSTRUCAO": "tipo_construcao",
    "TIPO DE TERRENO": "tipo_terreno",
    "QUANTIDADE DE ESQUINAS FRENTES": "quantidade_esquina_imovel",
    "QUANTIDADE DE ESQUINAS/FRENTES": "quantidade_esquina_imovel",
    "FRACAO IDEAL": "fracao_ideal",
    "AREA DO TERRENO": "area_terreno",
    "AREA CONSTRUIDA": "area_construida",
    "AREA OCUPADA": "area_ocupada",
    "VALOR DO M2 DO TERRENO": "valor_terreno",
    "VALOR DO M2 DE CONSTRUCAO": "valor_construcao",
    "ANO DA CONSTRUCAO CORRIGIDO": "ano_construcao_corrigida",
    "QUANTIDADE DE PAVIMENTOS": "quantidade_pavimento",
    "TESTADA PARA CALCULO": "testada_imovel",
    "ANO DE INICIO DA VIDA DO CONTRIBUINTE": "ano_inicio_vida_contribuinte",
    "MES DE INICIO DA VIDA DO CONTRIBUINTE": "mes_inicio_vida_contribuinte",
    "FASE DO CONTRIBUINTE": "fase_contribuinte",
    "FATOR DE OBSOLESCENCIA": "fator_obsolescencia",
}

# ordem do staging, sem a partição `ano`
COLUNAS_STAGING = [
    "data_cadastramento",
    "numero_contribuinte",
    "numero_notificacao",
    "numero_condominio",
    "codigo_logradouro",
    "logradouro",
    "numero_imovel",
    "cep",
    "complemento",
    "bairro",
    "referencia_imovel",
    "finalidade_imovel",
    "tipo_construcao",
    "tipo_terreno",
    "fracao_ideal",
    "area_terreno",
    "area_construida",
    "area_ocupada",
    "testada_imovel",
    "quantidade_esquina_imovel",
    "valor_terreno",
    "valor_construcao",
    "ano_construcao_corrigida",
    "quantidade_pavimento",
    "ano_inicio_vida_contribuinte",
    "mes_inicio_vida_contribuinte",
    "fase_contribuinte",
    "fator_obsolescencia",
]

# colunas com decimal
COLUNAS_DECIMAIS = [
    "fracao_ideal",
    "testada_imovel",
    "valor_terreno",
    "valor_construcao",
    "fator_obsolescencia",
]

# colunas inteiras: sem zeros à esquerda nem casas decimais
COLUNAS_INTEIRAS = [
    "numero_notificacao",
    "numero_imovel",
    "area_terreno",
    "area_construida",
    "area_ocupada",
    "quantidade_esquina_imovel",
    "ano_construcao_corrigida",
    "quantidade_pavimento",
    "ano_inicio_vida_contribuinte",
    "mes_inicio_vida_contribuinte",
]
