"""Constants for the br_mgi_pncp recurring pipeline (Prefect 3).

Portal Nacional de Contratações Públicas — procurement across federal, state and
municipal government, from the PNCP consulta REST API. See
``models/br_mgi_pncp/CLAUDE.md`` for the API traps this configuration encodes.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_mgi_pncp pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/br_mgi_pncp/code/``, which are the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "br_mgi_pncp"

    BASE_URL = "https://pncp.gov.br/api/consulta/v1/"
    USER_AGENT = (
        "Mozilla/5.0 (compatible; BaseDosDados/1.0; +https://basedosdados.org)"
    )

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "br_mgi_pncp" / "code" / "architecture"
    )

    # tamanhoPagina is floored at 10 everywhere, but its ceiling is PER
    # ENDPOINT and exceeding it is a flat 400, not a clamp. From the OpenAPI
    # spec (https://pncp.gov.br/api/consulta/v3/api-docs), verified 2026-08-28:
    #
    #     /v1/contratos, /v1/contratos/atualizacao      500
    #     /v1/atas, /v1/atas/atualizacao                500
    #     /v1/pca/atualizacao                           500
    #     /v1/instrumentoscobranca/inclusao             100
    #     /v1/contratacoes/publicacao, .../atualizacao   50
    #
    # This default is the common case; anything lower is declared per endpoint
    # as ENDPOINTS[...]["page_size"] and covered by a test.
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
            # Windows through 2024-05-24 are already harvested at 10 days and
            # their names are their windows, so that stretch is frozen. After
            # it, 10 days is too big: modalidade 6 and 8 reach 238-380 pages
            # in 2024+ and, under rate limiting, die deep -- page 153 of 238
            # after 63 minutes -- which used to discard the lot and split.
            # 2 days keeps every window near ~50 pages so it finishes.
            "resize": ("2024-05-25", 2),
            # BOTH contratacoes endpoints cap tamanhoPagina at 50, not the
            # 500 every other endpoint allows -- declared in the OpenAPI spec
            # at /api/consulta/v3/api-docs and enforced with a flat 400
            # "Tamanho de página inválido". Without this the table cannot be
            # harvested at all: every window fails on its first request.
            "page_size": 50,
            # PNCP's usable concurrency is not a constant, and this number
            # has already expired once. Measured 2026-09-03, each over 24-25
            # minutes on the same windows, right after an hour-long API
            # outage:
            #
            #     3 workers -> 139 pages/hr, 6 failures, pacer 0.55s
            #     2 workers ->   0 pages/hr, 1 failure,  3 splits
            #     1 worker  -> 458 pages/hr, 0 failures, pacer at its floor
            #
            # Concurrency was manufacturing 5xx, and each 5xx costs up to ~5
            # minutes of retry backoff, so two requests in flight did *less*
            # than one. On 2026-08-28 the same measurement said 3 was right
            # and 9 was too many -- that was true then. Re-measure before
            # changing this; do not reason from either number.
            "max_workers": 1,
        },
        "contrato": {
            "path": "contratos/atualizacao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 15,
            # 2021-01-01..2025-08-07 is already on disk as 112 contiguous
            # 15-day chunks, and a chunk's filename IS its window, so that
            # stretch can never be re-sized. Everything after it is still
            # un-harvested, and 15 days there means ~190 pages per window --
            # deep offsets, where a page costs ~14-20s against ~8s in the
            # first dozen. Measured on the live API, not assumed.
            #
            # 2025-08-08 is exactly the next window edge after the last
            # harvested chunk (20250724_20250807). Shifting it by one day
            # would renumber every later tag and silently re-download all 112.
            "resize": ("2025-08-08", 2),
        },
        # 7 days answers HTTP 500 ("Failed to obtain JDBC Connection",
        # Hikari pool exhausted) while the same request for a single day
        # returns fine -- measured 2026-08-28, ~1,090 records/day, so 2 days
        # is ~5 pages and stays in the cheap end of the latency curve.
        "ata_registro_preco": {
            "path": "atas/atualizacao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 2,
        },
        "instrumento_cobranca": {
            "path": "instrumentoscobranca/inclusao",
            "date_params": ("dataInicial", "dataFinal"),
            "by_modalidade": False,
            "window_days": 30,
            # 200 and 500 are rejected with 400 "Tamanho de página inválido".
            "page_size": 100,
            # The most fragile endpoint PNCP exposes: at 3 workers it failed
            # 3 of ~65 windows with 504s and dropped connections, while
            # contrato ran 305 windows at the same concurrency with zero
            # failures. The same windows serve fine when it is not being hit
            # in parallel, so the limit is on this endpoint, not on us.
            "max_workers": 1,
        },
        # NOTE: this endpoint paginates over *items*, not plans. Each page
        # returns one plan record carrying up to tamanhoPagina items, and
        # totalRegistros counts items. Successive pages carry disjoint item
        # slices of the same plan, so paging through and exploding on `itens`
        # yields each item exactly once — verified against pages 1 and 2 of
        # 2025-03-01..10, which shared zero numeroItem values.
        # WARNING: 7 is a FLOOR, not a tuning knob. A 1-day window answers
        # HTTP 200 with a zero-length body -- no error, no records -- while
        # the same request over 7 days returns 44,054 items across 89 pages
        # (measured 2026-06-01..07). Shrinking this the way ata_registro_preco
        # was shrunk yields a silently EMPTY table, not a slower one.
        #
        # Note also that dates here must be YYYYMMDD: the dashed form returns
        # the same empty body rather than a 400.
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

    # DEFERRED, not dropped. plano_contratacao_anual is ~76h of API time on
    # its own -- roughly twice the other four tables combined -- because
    # /v1/pca/atualizacao serves ~40s pages and the cheaper year-keyed
    # /v1/pca/ endpoint times out on every classification code tried. It is
    # backfilled as a separate follow-up run and added to the dataset then.
    #
    # Everything for it stays in place: its ENDPOINTS entry, architecture CSV,
    # flatten/EXPLODE handling and dicionario mapping. Only the harvest,
    # upload and dbt scope exclude it, so the onboarding PR carries no dbt
    # model whose staging table does not exist -- table-approve materialises
    # every model in a PR and would abort the whole run on that one.
    DEFERRED_TABLES = ["plano_contratacao_anual"]

    # Fact tables, smallest first — the order the upload and dbt steps use, so a
    # configuration problem surfaces on a cheap table.
    FACT_TABLES = [
        "instrumento_cobranca",
        "ata_registro_preco",
        "contratacao",
        "contrato",
    ]

    # dicionario is derived from the fact tables, so it is rebuilt last.
    ALL_TABLES = [
        "instrumento_cobranca",
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
    # the schedule again. See models/br_mgi_pncp/CLAUDE.md.
    LOOKBACK_DAYS = 10
