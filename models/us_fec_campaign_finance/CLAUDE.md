# us_fec_campaign_finance

FEC bulk campaign-finance data. The FEC republishes the **current** election cycle daily
and freezes past cycles, so a scheduled run re-pulls only the current cycle and
overwrites that one partition. Every frozen cycle stays in the staging bucket untouched,
and the dbt models — plain `materialized="table"` — rebuild the full 1980-present table
from all of them.

## Refresh cadence
- `0 5 * * 0` — 05:00 America/Sao_Paulo, Sun

Staging upload: dump mode `append`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `candidate` | `year` (int64) | table | AllFree | 16 |
| `candidate_committee_link` | `year` (int64) | table | AllFree | 8 |
| `committee` | `year` (int64) | table | AllFree | 16 |
| `committee_transaction` | `year` (int64) | table | PartBdpro | 24 |
| `contribution_committee` | `year` (int64) | table | PartBdpro | 25 |
| `contribution_individual` | `year` (int64) | table | PartBdpro | 24 |
| `dicionario` | — | table | — | 5 |
| `disbursement` | `year` (int64) | table | PartBdpro | 28 |

## Where the code lives
- `pipelines/datasets/us_fec_campaign_finance/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_fec_campaign_finance/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_fec_campaign_finance/code/architecture/` are the
  schema source of truth.

## Source
- https://basedosdados.org
- https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1

## Design notes
That is why the upload uses ``dump_mode="append"``. "overwrite" would delete the whole
staging table and, via ``tb.delete(mode="all")``, the prod table with it — throwing away
45 years of frozen cycles to refresh one. "append" with a deterministic blob path
(``staging/<ds>/<table>/year=<CYCLE>/data.parquet``) replaces exactly the current
cycle's partition, which is the intended semantics.

The four transaction tables are high-frequency, so they carry the BD Pro rolling window:
the most recent 6 months are pro-only, everything older is free. The registration tables
(candidate, committee, candidate_committee_link) have no date column and stay fully
free.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers
``us_fec_campaign_finance_flow``; the dev pool ignores the schedule, the prod pool
activates it.

No Prefect imports: the recurring pipeline in pipelines/datasets/us_fec_campaign_finance
imports these functions rather than duplicating them (.claude/rules/prefect-pipeline-
conventions.md, "DRY with the onboarding code").

The FEC publishes one ZIP per file type per two-year election cycle, each holding a
single pipe-delimited text file with no header row. Layouts are documented and stable;
the only per-file quirks are:

* ``oppexp`` carries a trailing delimiter, so every line has 26 fields for 25 documented
columns. The 26th is dropped. * ``oppexp`` dates are ``MM/DD/YYYY``; every other
transaction file uses ``MMDDYYYY``. * Files are not quoted and contain stray double
quotes inside names, so they must be parsed with QUOTE_NONE or lines get swallowed. *
Encoding is latin-1, not UTF-8.

## Operating reminders
- A `COMPLETED` run is not proof of an ingest: the source poll returns early and
  still completes. Check the logs, or run
  `uv run python -m pipelines.diagnostics health`.
- The dev materialization runs only when `materialize_to_prod=False`. That is the
  pre-arm validation path; an armed run goes straight to prod.
- Validate with
  `{"materialize_to_prod": false, "update_metadata": false, "force_run": true}`
  on the dev pool, and remember the PR needs the `deploy-flow` label to deploy at
  all.

<!-- Generated from constants.py / flows.py / the dbt models. Extend by hand with
     source-specific gotchas as they are discovered. -->
