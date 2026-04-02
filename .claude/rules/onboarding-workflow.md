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
7.  dbt-run              run DBT tests; fix or flag errors
8.  discover             resolve all reference IDs from backend (dev)
9.  metadata             register metadata in dev backend
[PAUSE — verification checkpoint]
10. metadata --env prod  promote to prod (only after human approval)
11. pr                   open PR with changelog
```

## Verification checkpoint (between steps 9 and 10)

After step 9 succeeds, output the following checklist and **wait for explicit approval** before proceeding to step 10:

```text
✓ Dataset registered in dev: <slug>
✓ Tables: <list>
✓ Columns: <counts per table>
✓ Coverage: <start>–<end>
✓ Cloud tables: OK
✓ Verify at: https://development.basedosdados.org/dataset/<slug>

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
