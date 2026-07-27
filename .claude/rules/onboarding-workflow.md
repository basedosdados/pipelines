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
9.  metadata             register metadata in dev backend (dataset status = under_review)
[PAUSE — verification checkpoint]
10. metadata --env prod  promote to prod (dataset status = under_review; only after human approval)
11. pr                   open PR with changelog
12. pipeline             recurring sources only — add a Prefect refresh pipeline
[PR MERGES + GH table-approve action runs + prod tables verified]
13. publish              flip the prod dataset status under_review → published
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

**A dev run is the definition of done — not a clean local check.** Local verification
(imports, transform parity, deploy discovery) cannot reach the parts that break: on
`us_bls_cpi` it passed while three separate bugs waited in the upload, the poll, and the
staging schema. Step 12 is not finished until the flow has run on the **dev pool** with
`{"materialize_to_prod": False, "update_metadata": False, "force_run": True}` and the logs
show `dbt run OK` + `dbt test OK` for every table. Two traps around that run:

- The PR needs the **`deploy-flow` label** or the staging deploy is `skipped` and nothing
  is deployed — silently.
- The defaults are `materialize_to_prod=True, update_metadata=True`, and the metadata
  tasks are pinned `env="prod"` even from the dev pool. A run triggered with `{}` writes
  **prod** data and metadata and applies the paywall.

**Green ≠ ingested.** The poll guard returns early and Prefect still reports `COMPLETED`,
so a dead pipeline looks healthy (`br_ibge_ipca`: 4 ingests in 60 completed runs). Read
the logs via `mcp__databasis__get_flow_run_logs`, or check whether coverage moved.

**Merging does not arm it.** The prod deploy lands `paused=True` and the backend sync
registers an unknown deployment with `is_schedule_active=False`. Arming is a manual tick
in Django admin (`/admin/admin_data_tools/disabledflowschedule/`), and the first armed run
is the first-ever execution of the prod upload — and, **only for a `part_bdpro` table**, of
the Row Access Policies (`needs_row_access_policy` is `isinstance(spec, PartBdpro)`, so an
all-free pipeline never issues them). Do it deliberately, watching.

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

## Dataset status lifecycle (create `under_review`, publish only post-merge)

**Every dataset is created with `status = under_review`, at every stage — dev/staging in step 9 and prod in step 10, never `published`.** `under_review` hides the dataset from the production frontend, so a dataset whose metadata is registered before its PR lands (and whose prod cloud tables do not yet exist) cannot leak publicly.

Turn the dataset to `status = published` **only in step 13, and only after all three hold**:

1. the onboarding **PR is merged** to `main`;
2. the GitHub **table-approve action has run successfully** (it materialises `basedosdados.<gcp_dataset_id>.*` via `dbt --target prod`; watch for the phantom-model failure mode where non-model `.sql` in the PR aborts materialisation before any real table builds);
3. the live prod tables **and** metadata are **verified** — row counts match, cloud tables resolve, and `get_dataset(slug, env="prod")` shows the expected shape.

Publishing is one call: `create_update_dataset(id=<dataset_id>, …, status_id=status.published, env="prod")` (re-pass every required field — the API does no partial updates). It is a **separate post-merge action**, independent of the optional recurring-pipeline step (12); never publish inside the onboarding PR, and never publish a dataset whose PR has not merged and materialised. Tables are gated by the dataset's status, so they may remain `published`; flipping the dataset makes everything go live in one step.

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
