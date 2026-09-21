"""Constants for the br_senado_dados_abertos_administrativos recurring pipeline.

Senado Federal administrative open data (``adm.senado.gov.br``). The recurring
pipeline reuses the onboarding cleaning transform (`utils.clean_all`) unchanged
and refreshes on two schedules — parents daily, the contratação fan-out weekly.
See ``models/br_senado_dados_abertos_administrativos/PIPELINE_PLAN.md``.

Table lists are not restated here: they come from the architecture spec via
``utils.ALL_TABLES / SNAPSHOTS / PARTITIONED``, which stays the single source of
truth.
"""

from enum import Enum


class constants(Enum):
    """Constants for the pipeline. Lowercase name follows the repo convention."""

    DATASET_ID = "br_senado_dados_abertos_administrativos"

    # The contratação children — built only on the weekly run, because they need
    # the ~27k-request status fan-out (`clean_all(sub_resources=True)`). Every
    # other table is built on the daily run. Kept in sync with the
    # `sub_resources` block of `utils.clean_all`.
    SUBRESOURCE_TABLES = (
        "contratacao_item",
        "contratacao_garantia",
        "contratacao_pagamento",
        "contratacao_documento_fiscal",
        "contratacao_pagamento_empenho",
        "contrato_aditivo",
        "ata_acionamento",
    )

    # Schedules — America/Sao_Paulo. Minute 17 chosen off the shared crontab so
    # this dataset does not pile onto minute 0 with everyone else. The source is
    # an always-on API with no publication window, so the hour is free; early
    # morning keeps it clear of the BigQuery daytime load.
    #
    # The two schedules are on mutually-exclusive days — daily every day except
    # Monday, weekly on Monday — so exactly one run fires per day and never two
    # at once. The Monday run carries the contratação fan-out (the children).
    DAILY_CRON = "17 6 * * 0,2,3,4,5,6"
    WEEKLY_CRON = "17 6 * * 1"

    # BD Pro rolling window: 6 months, on the snapshot tables only (data_extracao).
    FREE_LAG_MONTHS = 6
