# us_hhs_nppes

NPPES (National Plan and Provider Enumeration System) — the US registry of every
health care provider and organization holding an NPI, published by CMS. The
source republishes a **full replacement snapshot monthly**. We stack snapshots
(CNPJ model): each run uploads the new snapshot to staging with
`dump_mode="overwrite"` and the **incremental** dbt models append its
`extraction_date` partition to the prod tables, so history accumulates.

## Refresh cadence
- `23 15 8,10,12,14,16 * *` — 15:23 America/Sao_Paulo, on the days CMS typically
  posts the monthly bundle.

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`.

## Tables
| table | partition | materialization | coverage tier | columns |
|---|---|---|---|---|
| `provider` | `extraction_date` (date) | incremental | part_bdpro | 54 |
| `taxonomy` | `extraction_date` (date) | incremental | part_bdpro | 9 |
| `other_identifier` | `extraction_date` (date) | incremental | part_bdpro | 7 |
| `other_name` | `extraction_date` (date) | incremental | part_bdpro | 5 |
| `practice_location` | `extraction_date` (date) | incremental | part_bdpro | 11 |
| `endpoint` | `extraction_date` (date) | incremental | part_bdpro | 17 |
| `dicionario` | — | table | — | 5 |

## Where the code lives
- `pipelines/datasets/us_hhs_nppes/` — `constants.py` (URLs, table list),
  `utils.py` (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_hhs_nppes/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_hhs_nppes/code/architecture/` are the
  schema source of truth; the `.sql` models and `schema.yml` are **generated**
  from them by `code/build_dbt_models.py` and `code/build_schema_yml.py`. Edit
  `code/build_architecture.py`, then regenerate — do not hand-edit the outputs.
  Run `pre-commit` on the regenerated `.sql` afterwards; sqlfmt re-wraps the
  handful of over-long `safe_cast` lines.

## Source
- https://download.cms.gov/nppes/NPI_Files.html
- Monthly ZIP: `NPPES_Data_Dissemination_<Month>_<Year>_V2.zip` (~1.1 GB).
- Public domain (FOIA-disclosable under CMS-6060-N); registered as `cc0`.

## Design notes

The run polls cheaply first: it discovers the monthly link from the listing page,
HEADs it, and compares `Last-Modified` against `Table.Update.latest`. The ~1.1 GB
payload is downloaded only when CMS has actually republished, so a scheduled run
between monthly releases is a cheap no-op.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_hhs_nppes_flow`; the
dev pool ignores the schedule, the prod pool activates it (deployed paused).

Pure functions (no Prefect) so they are importable and testable. The recurring
pipeline wraps them in `@task` (see `tasks.py`); the bootstrap CLI imports
`clean_all` / `download_monthly` directly.

The main file is 11.6 GB and 330 columns wide. It is read in pyarrow record
batches — never loaded whole — and 275 of those columns are three repeating
groups (taxonomy×15, other identifier×50, taxonomy group×15) that are melted into
the `taxonomy` and `other_identifier` long tables. Output is **all-STRING**
hive-partitioned parquet: `upload_to_gcs` infers the staging schema from a
stringified header, so typed parquet is rejected; the dbt models `safe_cast`
every column back to its real type. `extraction_date` is encoded in the path
only, never in the file body.

Each partition directory starts with a 0-row `00_header.parquet`. It sorts ahead
of every data file, so the table-approve CI step reads a tiny file instead of
loading a large one whole — see `project_table_approve_parquet_header_oom`.

Every table is `dbt run` before any table is `dbt test`ed, per environment: the
`custom_dictionary_coverage` tests read the sibling `dicionario` model, and
interleaving run/test per table fails in a clean environment.

`taxonomy_code` labels are **not** shipped. The NUCC taxonomy code set is
AMA-copyrighted and requires a licence for commercial use; see the open decision
in `ONBOARDING_PLAN.md`.
