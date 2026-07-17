# Onboarding Workflow

Reference for the orchestrator and all worker agents. Defines the canonical 11-step sequence, quality gates, and commit discipline for Data Basis dataset onboarding.

## Full step sequence

Work through steps in order. Do not skip steps.

```
1.  context              gather raw source URLs, docs, org, coverage
2.  architecture         fetch or create architecture tables on Drive
3.  download             download raw data files to input/
4.  clean                write and run data cleaning code → partitioned parquet
5.  upload               upload parquet to BigQuery dev
6.  dbt                  write .sql and schema.yml files
7.  validate             run DBT tests and data quality checks; fix or flag errors
8.  discover             resolve all reference IDs from backend (dev)
9.  metadata             register metadata in dev backend
[PAUSE — verification checkpoint]
10. metadata --env prod  promote to prod (only after human approval)
11. pr                   open PR with changelog
12. pipeline             recurring sources only — add a Prefect refresh pipeline
```

## Step 12 — recurring pipeline (only for sources that update on a cadence)

Steps 1–11 land the data once. If the source republishes on a cadence (monthly,
daily, annual), add a Prefect 3 pipeline so it refreshes automatically. This is a
**separate, optional step** after the static onboarding is verified — one-off or
frozen datasets stop at step 11.

Follow `prefect-pipeline-conventions` (structure, flow recipe, shared
`pipelines/utils`, coverage types, scheduling, deploy). Use the `pipeline` agent
(skill `onboarding-pipeline`). The pipeline **reuses** the dbt models, architecture
CSVs, and cleaning transform from steps 2–6 — it does not redesign them; the
cleaning transform is shared with `models/<ds>/code/` rather than duplicated.

**Update and Poll records.** A recurring dataset must carry all three: the table `Update`
(when we last refreshed), the raw data source `Update` (the source's **max coverage
date**, not today), and the raw data source `Poll` (when we last looked). The flow writes
them only on a run with `update_metadata=True`, so create any that are missing by hand and
verify — a dataset tested with metadata off ends up with a Poll and no source Update.

**BD Pro rolling window.** Data Basis paywalls the most recent window of any table
that refreshes **monthly or more often**; older data stays free, and lower-frequency
tables in the same dataset stay free entirely. Decide the tier **per table**: the
high-frequency one gets `PartBdpro(free_lag=…)` (default 6 months), the rest
`AllFree`. This is not extra machinery — `register_table_materialization_task`
already rewrites both coverage ranges and re-issues the BigQuery Row Access Policies
every run, so the window rolls by itself and the dbt model is untouched. It does
require a **pro Coverage (`is_closed=True`) to exist on the table before the spec
changes**, or the run hard-fails at `assert_coverage_topology`. See the "BD Pro
rolling window" section of `prefect-pipeline-conventions` for the full mechanism,
the free/pro `is_closed` polarity, and what is verifiable locally.
The upload/dbt/metadata halves run on the deployed worker (prod is not exercisable
locally).

## Verification checkpoint (between steps 9 and 10)

After step 9 succeeds, output the following checklist and **wait for explicit approval** before proceeding to step 10:

```text
✓ Dataset registered in dev: <slug>
✓ Raw data sources: <list>
✓ Tables: <list>
✓ Columns: <counts per table>
✓ Coverage: <start>–<end>
✓ Cloud tables: OK
✓ Verify at: https://development.basedosdados.org/dataset/<id>

Table order set: <list in order, or "default">
OL order set per table: <summary, or "default">

Reply "approved" to promote to prod, or describe what needs fixing.
```

Do not proceed to step 10 without the user replying "approved" (or equivalent).

## Commit discipline

Commit after each logical unit completes:

- After architecture tables are created or validated
- After cleaning code is verified (subset output confirmed by user)
- After DBT files are written
- After metadata is registered in dev
- After metadata is promoted to prod

Use conventional commits: `feat(<dataset_slug>): <description>`

Examples:
- `feat(br_mma_cnuc): add architecture tables and cleaning code`
- `feat(br_mma_cnuc): add dbt models`
- `feat(br_mma_cnuc): register metadata in dev and prod`

Never commit data files (parquet, CSV). Ensure `.gitignore` covers the output path.

## Translation requirement

All names and descriptions must be provided in **Portuguese, English, and Spanish**. When only Portuguese is available (typical for Brazilian government datasets), translate to the other two languages using domain knowledge of Brazilian public administration and statistics. Apply consistent terminology across all tables in a dataset.

## Architecture table is the source of truth

When there is a conflict between raw data column names, DBT file conventions, and the architecture table, **the architecture table wins**. Update all other artifacts to match.

## Error escalation

- If a step fails with a recoverable error (missing file, wrong type), fix and retry.
- If a step fails with a structural error (wrong schema, backend API issue), report to user before retrying.
- If two retries fail, escalate to user with the full error and a proposed fix.
- Never silently skip a step.

## Environments

| Stage | Backend | GCP project |
|-------|---------|-------------|
| Steps 1–9 | `development.backend.basedosdados.org` | `basedosdados-dev` |
| Step 10 | `backend.basedosdados.org` | `basedosdados` |
| Step 11 | n/a | n/a |
