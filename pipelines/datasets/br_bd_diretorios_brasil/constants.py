"""
Constantes de br_bd_diretorios_brasil.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_bd_diretorios_brasil."""

    # Área de trabalho do pod. `input/` recebe o CSV do Catálogo, `output/` o
    # parquet que sobe para a staging.
    PATH = "/tmp/br_bd_diretorios_brasil/escola/"

    # Portal OBIEE do Inep que serve o Catálogo de Escolas. O primeiro GET no
    # painel entrega os cookies anônimos; o POST em `?Go` com `Action=Extract`
    # devolve o CSV da análise em `CATALOG_PATH`.
    DASHBOARD_URL = (
        "https://anonymousdata.inep.gov.br/analytics/saw.dll?dashboard"
    )
    GO_URL = "https://anonymousdata.inep.gov.br/analytics/saw.dll?Go"
    CATALOG_PATH = (
        "/shared/Censo da Educação Básica/Catálogo das Escolas/Análises"
        "/Lista das Escolas/Análise - Tabela da lista das escolas - Detalhado"
    )
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    )

    # O portal oscila: já devolveu conexão encerrada (curl com saída 35) e
    # HTTP 502 minutos antes de entregar o CSV inteiro. As duas falhas passam
    # numa nova tentativa, então cada download é repetido antes de desistir.
    # O `Extract` devolve no máximo 100.000 linhas, então o Catálogo é baixado
    # uma UF por vez. O filtro usa o nome interno da coluna: "UF" é só o rótulo
    # exibido, e filtrar por ele faz o portal ignorar o filtro sem avisar.
    EXTRACT_ROW_LIMIT = 100_000
    UF_FILTER_COLUMN = '"D - Localidade Escola"."Sigla Uf"'
    UFS = [
        "RO",
        "AC",
        "AM",
        "RR",
        "PA",
        "AP",
        "TO",
        "MA",
        "PI",
        "CE",
        "RN",
        "PB",
        "PE",
        "AL",
        "SE",
        "BA",
        "MG",
        "ES",
        "RJ",
        "SP",
        "PR",
        "SC",
        "RS",
        "MS",
        "MT",
        "GO",
        "DF",
    ]

    # Menor fração das escolas `Presente` do diretório publicado que o
    # Catálogo baixado precisa trazer para a carga seguir.
    MIN_CATALOG_SHARE = 0.95

    DOWNLOAD_ATTEMPTS = 3
    RETRY_WAIT_SECONDS = 15
    CURL_TIMEOUT_SECONDS = 600

    # Nome da coluna no CSV -> nome na staging.
    RENAME = {
        "Restrição de Atendimento": "restricao_atendimento",
        "Escola": "nome",
        "Código INEP": "id_escola",
        "UF": "sigla_uf",
        # Mantida só para derivar o `id_municipio`; não sobe para a staging.
        "Município": "nome_municipio",
        "Localização": "localizacao",
        "Localidade Diferenciada": "localidade_diferenciada",
        "Categoria Administrativa": "categoria_administrativa",
        "Endereço": "endereco",
        "Telefone": "telefone",
        "Dependência Administrativa": "dependencia_administrativa",
        "Categoria Escola Privada": "categoria_privada",
        "Conveniada Poder Público": "conveniada_poder_publico",
        "Regulamentação pelo Conselho de Educação": "regulacao_conselho_educacao",
        "Porte da Escola": "porte",
        "Etapas e Modalidade de Ensino Oferecidas": "etapas_modalidades_oferecidas",
        "Outras Ofertas Educacionais": "outras_ofertas_educacionais",
        "Latitude": "latitude",
        "Longitude": "longitude",
    }

    # Ordem das colunas na staging, espelhando o `.sql`.
    COLUMNS = [
        "id_escola",
        "nome",
        "id_municipio",
        "sigla_uf",
        "restricao_atendimento",
        "localizacao",
        "localidade_diferenciada",
        "categoria_administrativa",
        "endereco",
        "telefone",
        "dependencia_administrativa",
        "categoria_privada",
        "conveniada_poder_publico",
        "regulacao_conselho_educacao",
        "porte",
        "etapas_modalidades_oferecidas",
        "outras_ofertas_educacionais",
        "latitude",
        "longitude",
        "situacao_catalogo",
    ]

    # Se a escola ainda consta no Catálogo. Rótulos legíveis, não códigos,
    # para a coluna dispensar tabela de dicionário (o conjunto não tem uma).
    SITUACAO_CATALOGO = "situacao_catalogo"
    PRESENTE = "Presente"
    AUSENTE = "Ausente"

    # Municípios cujo nome no Catálogo difere do diretório `municipio` mesmo
    # depois de tirar os acentos (renomeações, reformas ortográficas, hífen).
    # (nome exato no Catálogo, sigla_uf) -> id_municipio de 7 dígitos.
    MUNICIPIO_NAME_FIXES = {
        ("Muquém do São Francisco", "BA"): "2922250",  # "do" → "de"
        ("Santa Terezinha", "BA"): "2928505",  # z → s (Teresinha)
        ("Itapajé", "CE"): "2306306",  # j → g (Itapagé)
        ("Barão do Monte Alto", "MG"): "3105509",  # "do" → "de"
        ("Dona Euzébia", "MG"): "3122900",  # z → s (Eusébia)
        ("Passa Vinte", "MG"): "3147808",  # espaço → hífen (Passa-Vinte)
        ("São Tomé das Letras", "MG"): "3165206",  # Tomé → Thomé
        ("Poxoréu", "MT"): "5107008",  # u → o (Poxoréo)
        ("Santo Antônio de Leverger", "MT"): "5107800",  # "de" → "do"
        ("Santa Izabel do Pará", "PA"): "1506500",  # z → s (Isabel)
        ("Iguaracy", "PE"): "2606903",  # y → i (Iguaraci)
        ("Arez", "RN"): "2401206",  # z → s (Arês)
        ("Assú", "RN"): "2400208",  # Assú → Açu
        ("Januário Cicco", "RN"): "2410306",  # hoje Serra Caiada
        ("Olho d'Água do Borges", "RN"): "2408409",  # espaço → hífen
        ("São Luiz do Anauá", "RR"): "1400605",  # no diretório, "São Luiz"
        ("Grão-Pará", "SC"): "4206108",  # hífen → espaço (Grão Pará)
        ("Amparo do São Francisco", "SE"): "2800100",  # "do" → "de"
        ("Graccho Cardoso", "SE"): "2802601",  # cc → c (Gracho Cardoso)
        ("Biritiba Mirim", "SP"): "3506607",  # espaço → hífen
        ("Florínea", "SP"): "3516101",  # nea → nia (Florínia)
        ("São Luiz do Paraitinga", "SP"): "3550001",  # z → s (São Luís)
        ("Tabocão", "TO"): "1708254",  # hoje Fortaleza do Tabocão
    }

    # Atributos que as escolas vindas só do Censo Escolar recebem. Coluna no
    # Censo -> (coluna no diretório, código -> rótulo do Catálogo). As chaves
    # são texto porque os códigos chegam do BigQuery como texto.
    CENSO_TO_DIRECTORY = {
        "rede": (
            "dependencia_administrativa",
            {
                "1": "Federal",
                "2": "Estadual",
                "3": "Municipal",
                "4": "Privada",
            },
        ),
        "tipo_localizacao": ("localizacao", {"1": "Urbana", "2": "Rural"}),
        "tipo_localizacao_diferenciada": (
            "localidade_diferenciada",
            {
                "0": "A escola não está em área de localização diferenciada",
                "1": "Área de assentamento",
                "2": "Terra indígena",
                "3": "Área remanescente de quilombos",
                "8": "Área onde se localizam povos e comunidades tradicionais",
            },
        ),
        "tipo_categoria_escola_privada": (
            "categoria_privada",
            {
                "1": "Particular",
                "2": "Comunitária",
                "3": "Confessional",
                "4": "Filantrópica",
            },
        ),
        "conveniada_poder_publico": (
            "conveniada_poder_publico",
            {"0": "Não", "1": "Sim"},
        ),
        "tipo_regulamentacao": (
            "regulacao_conselho_educacao",
            {"0": "Não", "1": "Sim", "2": "Em Tramitação"},
        ),
    }
