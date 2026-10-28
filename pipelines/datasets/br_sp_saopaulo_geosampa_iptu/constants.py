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
