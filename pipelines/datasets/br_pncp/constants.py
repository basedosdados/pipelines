"""Constants for the br_pncp recurring pipeline (Prefect 3).

Portal Nacional de Contratações Públicas — procurement across federal, state and
municipal government, from the PNCP consulta REST API. See
``models/br_pncp/CLAUDE.md`` for the API traps this configuration encodes.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_pncp pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/br_pncp/code/``, which are the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "br_pncp"

    BASE_URL = "https://pncp.gov.br/api/consulta/v1/"
    USER_AGENT = (
        "Mozilla/5.0 (compatible; BaseDosDados/1.0; +https://basedosdados.org)"
    )

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "br_pncp" / "code" / "architecture"
    )

    # tamanhoPagina is capped at 500 on most endpoints (and floored at 10), but
    # instrumentoscobranca caps at 100 and answers 400 "Tamanho de página
    # inválido" above it. The cap is per endpoint; see ENDPOINTS["page_size"].
    PAGE_SIZE = 500

    # PNCP publishes from 2021 (Lei 14.133/2021).
    START_YEAR = 2021

    # codigoModalidadeContratacao is mandatory on the contratacoes endpoints, so
    # every date window must be crossed with the full modalidade domain.
    MODALIDADES = list(range(1, 15))

    # Per-endpoint harvest configuration.
    #
    # ``path`` — every table is harvested from an *atualização* (update-date)
    # endpoint where one exists, so a run picks up amendments to older records as
    # well as new ones. For atas this is not merely preferable but required:
    # ``/v1/atas`` filters on the ata's vigência period, so a single day returns
    # the entire live stock (365,742 records) rather than that day's
    # publications.
    #
    # ``window_days`` — the initial window size. ``fetch_range`` halves any
    # window the server refuses, so these are starting points, not hard limits.
    #
    # Size them so a window stays SHALLOW, not merely servable. Per-page latency
    # grows with offset depth: measured ~7s/page on 30-page windows against
    # ~21s/page on 190-page ones, so a window three times larger costs far more
    # than three times as much. contrato keeps 15 only because 111 chunks were
    # already harvested at that size and the window size is baked into the chunk
    # filenames — re-sizing it would discard that work.
    ENDPOINTS = {
        "contratacao": {
            "path": "contratacoes/atualizacao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": True,
            "window_days": 10,
        },
        "contrato": {
            "path": "contratos/atualizacao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 15,
        },
        "ata_registro_preco": {
            "path": "atas/atualizacao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 7,
        },
        "instrumento_cobranca": {
            "path": "instrumentoscobranca/inclusao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 30,
            # 200 and 500 are rejected with 400 "Tamanho de página inválido".
            "page_size": 100,
        },
        # NOTE: this endpoint paginates over *items*, not plans. Each page
        # returns one plan record carrying up to tamanhoPagina items, and
        # totalRegistros counts items. Successive pages carry disjoint item
        # slices of the same plan, so paging through and exploding on `itens`
        # yields each item exactly once — verified against pages 1 and 2 of
        # 2025-03-01..10, which shared zero numeroItem values.
        "plano_contratacao_anual": {
            "path": "pca/atualizacao",
            "date_params": ("dataInicio", "dataFim"),
            "by_modalidade": False,
            "window_days": 7,
        },
    }

    # The endpoints used for the one-shot historical backfill. contratacao and
    # contrato are harvested by *publication* date there, because the backfill
    # wants each record filed under the year it was published and the
    # publication endpoints page more predictably over a fixed history.
    BACKFILL_PATHS = {
        "contratacao": "contratacoes/publicacao",
        "contrato": "contratos",
    }

    # Fact tables, smallest first — the order the upload and dbt steps use, so a
    # configuration problem surfaces on a cheap table.
    FACT_TABLES = [
        "instrumento_cobranca",
        "plano_contratacao_anual",
        "ata_registro_preco",
        "contratacao",
        "contrato",
    ]

    # dicionario is derived from the fact tables, so it is rebuilt last.
    ALL_TABLES = [
        "instrumento_cobranca",
        "plano_contratacao_anual",
        "ata_registro_preco",
        "contratacao",
        "contrato",
        "dicionario",
    ]

    # How far back a scheduled run re-harvests. PNCP backdates amendments, so a
    # window wider than the schedule interval is deliberate: overlapping runs are
    # idempotent because the dbt models deduplicate on the PNCP control number,
    # keeping the row with the latest data_atualizacao.
    #
    # Measured cost at 10 days: ~520 pages, ~25 minutes of API time.
    #
    # WARNING: an outage longer than this leaves a *permanent* gap. The window is
    # keyed on update date, so a record published outside it and untouched since
    # is never fetched, and deduplication cannot recover what was never
    # downloaded — the run still reports success. After any outage longer than
    # this, trigger a catch-up run with a wider `lookback_days` before trusting
    # the schedule again. See models/br_pncp/CLAUDE.md.
    LOOKBACK_DAYS = 10
