"""Shared configuration for the world_wb_mides acquisition code.

One module per state lives beside this one: `download_<uf>.py` fetches raw files into
`INPUT_DIR/<uf>/`, `clean_<uf>.py` converts them to parquet under `OUTPUT_DIR/<mirror>/`,
and `validate_<uf>.py` checks the result before anyone uploads it. The Prefect flow
imports them through `pipelines/datasets/world_wb_mides/utils.py`; nothing here imports
Prefect, so every module runs standalone from a laptop.

DATA_DIR is read from the environment AT IMPORT TIME, which is why the flow sets
`MIDES_DATA_DIR` before importing anything from this package.
"""

from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(
    os.environ.get("MIDES_DATA_DIR", Path(__file__).resolve().parent / "data")
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# Several TCE portals answer a bare urllib agent with a challenge page or a 403.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Serial, paced. Every portal in this dataset belongs to the family that answers a
# burst with an IP block lasting tens of minutes, so no downloader may use a worker
# pool. One request per second is the floor, and a state with a per-request quota
# raises its own.
REQUEST_INTERVAL_SECONDS = 1.0

# ---------------------------------------------------------------------------
# Paraná -- TCE-PR
# ---------------------------------------------------------------------------
# The host in the published documentation (servicos.tce.pr.gov.br) is DECOMMISSIONED
# and answers 503 site-wide behind a static JS redirect. The portal moved to:
PR_BASE = "https://pit.tce.pr.gov.br"
# One ZIP per exercise covering all 399 municipalities and all three phases. The whole
# 2022-2026 backfill is five requests.
PR_YEAR_ZIP = PR_BASE + "/Arquivos/{year}_PIT_TodosArquivos.zip"
PR_FIRST_YEAR = 2013
# The source switched encoding between our last harvest and now.
PR_ENCODING_BY_YEAR = {"<=2021": "utf-16-le", ">=2022": "utf-8"}

# ---------------------------------------------------------------------------
# Distrito Federal
# ---------------------------------------------------------------------------
# One static ZIP per exercise, carrying all three phases. 0.90 GB for 2009-2026.
DF_YEAR_ZIP = "https://www.transparencia.df.gov.br/arquivos/Despesa_{year}.zip"
DF_FIRST_YEAR = 2009
# EVERY exercise is regenerated nightly, not just the open one, so a window scoped to
# the current year goes silently stale against revisions to earlier ones. DF is cheap
# enough to re-fetch whole every run.
DF_REFETCH_ALL = True
# Liquidação's `emissao` is dd/mm/yyyy at source but ISO in the existing staging
# parquet. The dbt model applies a bare safe_cast, so a verbatim reload NULLs `data`
# and `mes` for every liquidação row. The cleaner must normalise to ISO.
DF_DATE_COLUMNS_DDMMYYYY = {"liquidacao": ["emissao"]}

# ---------------------------------------------------------------------------
# Minas Gerais -- TCE-MG
# ---------------------------------------------------------------------------
# `dadosabertos.tce.mg.gov.br` is only the Angular SPA. It serves no data: every path
# under it answers with the app shell or a Tomcat 404. The data lives on a separate WSO2
# gateway, on a non-standard port, behind a different path.
MG_BASE = "https://dadosabertos.tce.mg.gov.br"
MG_API = (
    "https://arabiasaudita.tce.mg.gov.br:8443"
    "/TCEMG-proxy-web/publico/wso2amgw/dados-abertos/dadosAbertos"
)
# A bulk package covering all 853 municipalities. Three requests per exercise -- one
# category lookup and two packages -- so 2022-2026 is fifteen.
MG_PACKAGE = MG_API + "/baixarArquivoPct/{seq_zip}"
MG_CATEGORIES_URL = MG_API + "/buscarCategoriaDownload"
MG_STATS_URL = MG_API + "/buscarEstatisticas"
# Not a secret. It is published verbatim in the SPA's own JS bundle
# (`main.*.js`, key `bearer`) and is the same for every anonymous visitor. It is NOT
# sufficient on its own: the gateway answers 401 until a second header,
# `AuthorizationProxy: token <JWT>`, accompanies it. See TCE_MG_CREDENTIAL_REQUEST.md.
MG_STATIC_BEARER = "3ab85573-29ed-3918-8fcd-9af980367c5b"
# Display labels returned by `buscarCategoriaDownload`, mapped to the phases each
# package carries. `seqZip` changes every exercise and must be resolved from that
# endpoint per run -- a cached one silently fetches the wrong year.
MG_CATEGORY_PHASES = {
    "Empenhos": ("empenho", "rsp"),
    "Despesas": ("liquidacao", "pagamento"),
}
MG_FIRST_YEAR = 2014
MG_MUNICIPALITIES = 853
# Both TCE-MG hosts serve an INCOMPLETE certificate chain: the leaf only, with the
# Sectigo OV R36 intermediate omitted. macOS `curl` papers over it from the system
# keychain; Python with certifi does not, and fails every request with
# CERTIFICATE_VERIFY_FAILED. The intermediate is pinned beside this module.
MG_CA_BUNDLE = (
    Path(__file__).resolve().parent
    / "certs"
    / "sectigo_public_server_authentication_ca_ov_r36.pem"
)
# `id_municipio` for liquidacao, pagamento and rsp is carried by the ZIP FILENAME, not
# by any column in the CSV. Losing it while unpacking silently drops the municipality
# key for three of MG's four mirrors.
MG_MUNICIPALITY_FROM_FILENAME = ["liquidacao", "pagamento", "rsp"]

# ---------------------------------------------------------------------------
# Ceará -- TCE-CE
# ---------------------------------------------------------------------------
# The documented host (api.tce.ce.gov.br) returns 500 on every path -- an SSL handshake
# failure to its own backend, not a WAF. The live replacement:
CE_BASE = "https://api-dados-abertos.tce.ce.gov.br/sim"
CE_FIRST_YEAR = 2009
# The exercise parameter is YYYY00, not YYYY. Passing a bare year returns HTTP 200 and
# an EMPTY ARRAY -- a silent wrong answer that looks like an exercise with no data.
CE_EXERCICIO = "{year}00"
# Renames since the models were written. Nothing the models need has been withdrawn, so
# the cleaner maps back to the old names and no dbt model changes.
CE_COLUMN_RENAMES = {
    "codigo_unidade_orcamentaria": "codigo_unidade",
    "descricao_historico_empenho": "descricao_empenho",
}
# `data_referencia_doc` must be YYYYMM exactly. A bare YYYY returns HTTP 200 and an EMPTY
# ARRAY -- the same silent-wrong-answer shape as the bare exercise year, and the failure a
# reader would reach for first when trying to collapse the month sweep. Nothing coarser
# works: `2021*` and `*` are rejected as invalid, ranges and operators are HTTP 400.
# Measured 2026-09-21, and the month fan-out is therefore irreducible. So is the
# municipality one: dropping `codigo_municipio` returns HTTP 400, "No search methods ready
# to be run. The following fields are obligatory: codigo_municipio". Of the three filters
# the API does accept -- codigo_municipio, exercicio_orcamento, codigo_orgao -- only
# `data_referencia_doc` selects the wide projection; codigo_orgao returns the narrow one.
# `data_referencia_doc` (YYYYMM) is NOT optional, whatever the survey and the spec say.
# Omitting it returns HTTP 200 with the right rows under a TRUNCATED projection --
# 12 columns instead of 43 on notas_empenhos -- dropping valor_empenhado, valor_liquidado,
# valor_nota_pagamento and every classification the dbt models read. So the harvest sweeps
# reference months, and the fan-out is municipality x exercise x month x endpoint.
#
# A record's reference month is NOT bounded by its exercise. Measured across the published
# 2009-2021 mirror: empenho and anulacao never spill (max offset 11 months for all 575,184
# and 109,893 rows of exercise 2021), but liquidacao and pagamento do, and the tail is long
# -- exercise 2009 pagamento carries rows at offset 131, i.e. reference months in 2020.
# Share of rows captured by a window of N months from the exercise start:
#
#     window       12       18       24       36       60
#     pagamento    92.8%    97.5%    99.6%    99.9%    99.95%   (worst exercise)
#     liquidacao   97.7%    99.5%    99.8%    99.95%   100%     (worst exercise)
#
# So no fixed window is provably complete. These are only the STARTING window: for the
# three endpoints that accept the parameter-less form, download_ce.py then uses that form
# as an exact completeness oracle and widens until the sweep is provably whole.
CE_REFERENCE_WINDOW_MONTHS = {
    "empenho": 12,
    "anulacao": 12,
    "liquidacao": 24,
    "pagamento": 24,
}
CE_REFERENCE_WINDOW_STEP = 12
# 2009's pagamento tail reaches offset 131. The cap is above that with room to spare; a
# sweep that reaches it without the oracle going quiet is a fault, not a long tail.
CE_REFERENCE_WINDOW_MAX = 180
# `notas_anulacoes_empenhos` rejects the parameter-less form with HTTP 400, so it has no
# oracle. Its window is fixed, which is safe only because its spill is measured at zero.
CE_ORACLE_MIRRORS = ("empenho", "liquidacao", "pagamento")
