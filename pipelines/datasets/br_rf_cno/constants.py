"""Constant values for br_rf_cno.

**CNO** (Cadastro Nacional de Obras), Receita Federal. The source publishes a
single non-decomposable ZIP (``cno.zip``, ~306 MB) containing the CSVs for
all 4 published tables (microdados, vinculos, areas, cnaes) in one pass —
see ``pipelines/datasets/br_rf_cno/README.md`` for the WAF access
workarounds and the per-table data quality notes. ``totais`` has a rename
entry in the ``constants`` enum below but no flow/dbt model, and
``dicionario`` is a static table with no CSV in the zip — neither is part
of this pipeline.

Each table has its own check_update/extract_and_load pair (the repo-wide
one-flow-per-table rule — see ``pipeline_factory`` in
``pipelines.utils.stage_dispatch``, used the same way by ``br_ibge_ipca``/
``br_ms_cnes``), even though that means re-downloading the same zip up to 4x
per cycle: accepted redundancy for uniformity, since the 4 tables don't
genuinely depend on each other (no cross-table dbt test tying them
together).

Originally split across ``pipelines.shared.rf`` (the shared crawler
constants, below as the ``constants`` enum) and this module (the dataset's
own plain constants, above); merged here since ``br_rf_cno`` is the
crawler's only consumer.
"""

from enum import Enum

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro

DATASET_ID = "br_rf_cno"

# As 4 tabelas publicadas (ver README's arquivo->tabela map). Exclui
# `totais` (tem rename em `constants.TABLES_RENAME` abaixo mas sem
# flow/modelo dbt) e `dicionario` (tabela estática, sem CSV no zip).
MICRODADOS_TABLE_ID = "microdados"
VINCULOS_TABLE_ID = "vinculos"
AREAS_TABLE_ID = "areas"
CNAES_TABLE_ID = "cnaes"

ALL_TABLES = [
    MICRODADOS_TABLE_ID,
    VINCULOS_TABLE_ID,
    AREAS_TABLE_ID,
    CNAES_TABLE_ID,
]

# Mesmo chunksize que os flows antigos por tabela passavam pro
# `process_file`.
CHUNKSIZE = 100000

# Mesma coverage pras 4 tabelas: incremental, particionada por
# `data_extracao` (DATE) — idêntica ao
# `PartBdpro(date_column=DateOnly(col="data_extracao"),
# date_format=DateFormat.YEAR_MD)` que o `_run_rf` antigo (removido; era o
# flow monolítico em `pipelines/shared/rf/flows.py`, substituído pelo
# `pipeline_factory` em `tasks.py`) passava pra toda table_id — mesma
# coluna de data, mesmo `free_lag` default (6 meses), não é uma decisão
# nova.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)


class constants(Enum):
    """Shared constants for the Receita Federal CNO crawler tasks
    (``tasks.py``/``utils.py``): request headers/body and source URLs, plus
    the per-table file/column rename maps.
    """

    XML_BODY = """<?xml version="1.0" encoding="utf-8" ?>
              <d:propfind xmlns:d="DAV:">
                <d:allprop/>
              </d:propfind>
              """

    HEADERS = {
        "Depth": "1",
        "Content-Type": "application/xml",
        "Accept": "application/xml",
        "User-Agent": "Mozilla/5.0",
    }
    URLS = {
        "url_base": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/",
        "url_download": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/cno.zip",
    }

    TABLES_RENAME = {
        "br_rf_cno": {
            "microdados": "cno.csv",
            "totais": "cno_totais.csv",
            "vinculos": "cno_vinculos.csv",
            "areas": "cno_areas.csv",
            "cnaes": "cno_cnaes.csv",
        }
    }

    COLUMNS_RENAME = {
        "br_rf_cno": {
            "vinculos": {
                "Data de registro": "data_registro",
                "Data de início": "data_inicio",
                "Data de fim": "data_fim",
                "CNO": "id_cno",
                "NI do responsável": "id_responsavel",
                "Qualificação do contribuinte": "qualificacao_contribuinte",
            },
            "cnaes": {
                "Data de registro": "data_registro",
                "CNO": "id_cno",
                "CNAE": "cnae_2_subclasse",
            },
            "microdados": {
                "Data da situação": "data_situacao",
                "Data de registro": "data_registro",
                "Data de início": "data_inicio",
                "Data de inicio da responsabilidade": "data_inicio_responsabilidade",
                "Código do Pais": "id_pais",
                "Nome do pais": "nome_pais",
                "Estado": "sigla_uf",
                "Código do municipio": "id_municipio_rf",
                "Bairro": "bairro",
                "CEP": "cep",
                "Logradouro": "logradouro",
                "Tipo de logradouro": "tipo_logradouro",
                "Número do logradouro": "numero_logradouro",
                "Caixa Postal": "caixa_postal",
                "CNO": "id_cno",
                "CNO vinculado": "id_cno_vinculado",
                "Situação": "situacao",
                "Complemento": "complemento",
                "NI do responsável": "id_responsavel",
                "Nome": "nome_responsavel",
                "Qualificação do responsavel": "qualificacao_responsavel",
                "Nome empresarial": "nome_empresarial",
                "Área total": "area",
                "Unidade de medida": "unidade_medida",
                "Código de localização": "id_localizacao",
            },
            "areas": {
                "CNO": "id_cno",
                "Categoria": "categoria",
                "Destinação": "destinacao",
                "Tipo de obra": "tipo_obra",
                "Tipo de Área": "tipo_area",
                "Tipo de Área Complementar": "tipo_area_complementar",
                "Metragem": "metragem",
            },
        }
    }
