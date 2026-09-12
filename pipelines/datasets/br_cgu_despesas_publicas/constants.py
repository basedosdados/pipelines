"""Constants for br_cgu_despesas_publicas — execução mensal da despesa federal.

Portal da Transparência (CGU) publishes one ZIP per month under
``despesas-execucao``, holding a single Latin-1, semicolon-separated CSV with 47
columns. Coverage runs 2014-01 to the current month; 2013-12 and earlier answer
403, which is how the lower bound is discovered rather than assumed.

Two source properties that shape everything downstream:

* **The 47-column layout is stable.** Header hashes are identical from 2014-01
  through 2026-07, so no per-vintage parsing is needed.
* **Each month is restated on its own schedule.** Unlike ``orcamento-despesa``,
  where every exercise file shares one ``Last-Modified``, here the timestamps
  differ per month (2014-01 last touched 2024-06-24, 2024-01 on 2026-08-26,
  2026-01 on 2026-09-04). So one HEAD does *not* stand for the whole release.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_cgu_despesas_publicas pipeline."""

    DATASET_ID = "br_cgu_despesas_publicas"
    TABLE_ID = "execucao"

    BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/despesas-execucao"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 rdahis@basedosdados.org"
    )

    # Earliest month the portal publishes. 2013-12 and earlier return 403.
    FIRST_YEAR = 2014
    FIRST_MONTH = 1

    SOURCE_ENCODING = "latin-1"
    SOURCE_DELIMITER = ";"

    # The source column that carries "AAAA/MM" and is split into ano + mes.
    PERIOD_COLUMN = "Ano e mês do lançamento"

    # Pacing for the CGU download host, which sits behind an AWS WAF rate rule.
    #
    # Measured 2026-09-11: 22 successful GETs spaced 6 s apart, spanning 3m54s,
    # then the 23rd came back 405 + x-amzn-waf-action: captcha. So the budget is
    # roughly 22 requests per ~4 minutes — about 5.6/min, where a flat 6 s delay
    # is 10/min. A flat delay alone therefore cannot carry a 150-month pull.
    #
    # Instead: REQUEST_DELAY within a batch, then BATCH_PAUSE between batches,
    # which keeps the sustained rate near 1.9/min, a third of the measured budget.
    # A challenge that still gets
    # through is caught and retried after BLOCK_PAUSE, which comfortably
    # outlasts the 6-9 minute block.
    REQUEST_DELAY = 6.0
    BATCH_SIZE = 15
    BATCH_PAUSE = 300.0
    BLOCK_PAUSE = 720.0
    MAX_BLOCK_RETRIES = 6

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "br_cgu_despesas_publicas"
        / "code"
        / "architecture"
    )
