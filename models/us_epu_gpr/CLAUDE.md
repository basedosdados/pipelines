# us_epu_gpr

Economic Policy Uncertainty (EPU; Baker, Bloom and Davis) + Geopolitical Risk (GPR;
Caldara and Iacoviello). Two newspaper-based uncertainty families on one long axis.
Both sources revise their full history whenever article counts are recomputed, so each
refresh is a **full replace** (dump_mode="overwrite"), not an append. A single flow
downloads every source and rebuilds all three tables.

## Refresh cadence
- `33 16 4,5,6,7,8,9,10 * *` — 16:33 America/Sao_Paulo, days 4–10 (GPR ~1st, EPU by ~8th)

Staging upload: dump mode `overwrite`, source format `parquet`.

## Tables
| table | partition | materialization | coverage tier | columns |
|---|---|---|---|---|
| `index_monthly` | `year` (int64) | table | AllFree | 6 |
| `index_daily` | `year` (int64) | table | AllFree | 8 |
| `dicionario` | — | table | — | 5 |

Long schema: `country_id` (ISO3, NULL for global aggregates), `index_family` (epu/gpr),
`index_name` (coded, dictionary-covered), `value`. `index_daily` adds `day` + `date`.
All tables are AllFree — the data is freely licensed and re-downloadable; to add a BD
Pro rolling window later, create a pro Coverage on `index_monthly` and switch its spec
to `PartBdpro`.

## Where the code lives
- `pipelines/datasets/us_epu_gpr/` — `constants.py`, `utils.py` (pure download + cleaning
  transform), `tasks.py`, `flows.py`.
- `models/us_epu_gpr/` — dbt models + `schema.yml`; architecture CSVs under
  `code/architecture/` are the schema source of truth.

## Sources
- EPU: https://www.policyuncertainty.com/ (CC BY 4.0) — WAF blocks HTML pages but allows
  direct `/media/*` file downloads.
- GPR: https://www.matteoiacoviello.com/gpr.htm (`.htm`, not `.html`) — free with citation
  to Caldara & Iacoviello (2022, AER 112(4)).

## Operating reminders
- A `COMPLETED` run is not proof of an ingest: the source poll returns early and still
  completes. Read the logs or check whether coverage moved.
- Dev materialization runs only when `materialize_to_prod=False` (the pre-arm validation
  path). Validate with `{"materialize_to_prod": false, "update_metadata": false,
  "force_run": true}` on the dev pool; the PR needs the `deploy-flow` label to deploy.
- Each data table links only the EPU raw source (the one-source-per-table poll limitation);
  the GPR source should be re-linked once that client bug is fixed.
