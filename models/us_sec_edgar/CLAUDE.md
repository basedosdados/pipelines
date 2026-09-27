# us_sec_edgar

SEC EDGAR Financial Statement Data Sets. The SEC publishes one ZIP per calendar quarter,
roughly five weeks after quarter end, and never rewrites an earlier one — so each run
**appends one partition** (``dump_mode="append"``) rather than replacing the history the
way us_bls_cpi does.

## Refresh cadence
- `0 16 5,8,11,14,17,20 1,4,7,10 *` — 16:00 America/Sao_Paulo, day 5,8,11,14,17,20, Jan/Apr/Jul/Oct

Staging upload: dump mode `append`, `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `dicionario` | — | table | — | 5 |
| `numeric_fact` | `year` (int64) | table | — | 12 |
| `presentation` | `year` (int64) | table | — | 12 |
| `submission` | `year` (int64) | table | — | 38 |
| `tag` | `year` (int64) | table | — | 11 |

## Where the code lives
- `pipelines/datasets/us_sec_edgar/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_sec_edgar/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_sec_edgar/code/architecture/` are the
  schema source of truth.

## Source
- https://www.sec.gov/data-research/sec-markets-data/
- https://www.sec.gov/files/dera/data/financial-statement-data-sets/
- https://www.sec.gov/os/webmaster-faq#developers

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_sec_edgar_flow`; the dev
pool ignores the schedule, the prod pool activates it.

No Prefect imports here: `models/us_sec_edgar/code/clean.py` (the one-shot onboarding
bootstrap) and `tasks.py` (the recurring pipeline) both import these, so the cleaning
transform exists in exactly one place.

Source: SEC Financial Statement Data Sets, one ZIP per calendar quarter holding four
tab-delimited files (sub, num, tag, pre). The published tables mirror them one-to-one,
stacked by release quarter (`year`, `quarter`).

Staging parquet is written **all-STRING** with the column order taken from the
architecture CSVs; the dbt models `safe_cast` each column to its real type. See the
"Staging parquet must be all-STRING" note in `.claude/rules/prefect-pipeline-
conventions.md`.

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
