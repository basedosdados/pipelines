---
name: onboarding
description: Orchestrates the full dataset onboarding workflow for Data Basis — from raw data → clean data → BigQuery → metadata in the backend. Spawn this agent when the user asks to onboard a dataset.
---

# Data Basis Onboarding Agent

This agent orchestrates the full dataset onboarding workflow for Data Basis: from raw data → clean data → BigQuery → metadata in the backend.

## MCP server

All Data Basis backend API calls go through the `mcp` MCP server. Always use the tools for backend operations — never write raw HTTP requests inline.

## How to invoke

The agent responds to natural-language onboarding requests. Canonical form:

```text
Onboard dataset <slug>. Raw files at <path>. Drive folder: BD/Dados/Conjuntos/<slug>/.
```

Anything missing from the request is gathered in `onboarding-context` before proceeding.

## Step sequence

Work through these steps in order. Do not skip steps. Use the corresponding skill for each step.

```text
1. /onboarding-context       gather raw source URLs, docs, org, license, coverage
2. /onboarding-architecture  fetch or create architecture tables on Drive
3. /onboarding-clean         write and run data cleaning code → partitioned parquet
4. /onboarding-upload        upload parquet to BigQuery (dev first)
5. /onboarding-dbt           write .sql and schema.yaml files
6. /onboarding-dbt-run       run DBT tests; fix or flag errors
7. /onboarding-discover      resolve all reference IDs from backend
8. /onboarding-metadata      register metadata in dev backend
[PAUSE — verification checkpoint]
9. /onboarding-metadata --env prod   (only after human approval)
10. /onboarding-pr            open PR with changelog
```

## Verification checkpoint (between steps 8 and 9)

After step 8 succeeds, output a verification checklist and wait for explicit approval before promoting to prod:

```text
✓ Dataset registered in dev: <slug>
✓ Tables: <list>
✓ Columns: <counts>
✓ Coverage: <start>–<end>
✓ Cloud tables: OK
✓ Verify at: https://development.basedosdados.org/dataset/<slug>

Table order set: <list in order, or "default">
OL order set per table: <summary, or "default">

Reply "approved" to promote to prod, or describe what needs fixing.
```

Note: ordering steps are handled inside `onboarding-metadata` (steps 7–8 of that command). If the human did not specify an order during step 8, you may ask here whether they want to adjust ordering before promoting to prod.

Do not proceed to step 9 or step 10 without the user replying "approved" (or equivalent).

## Environment

- Default: dev (`development.backend.basedosdados.org`, `basedosdados-dev` GCP project)
- Prod: only after explicit user approval

## Commit discipline

Commit after each logical unit completes:
- After cleaning code is verified
- After DBT files are written
- After metadata is registered in dev
- After metadata is promoted to prod

Use conventional commits: `feat(br_me_siconfi): add architecture tables and cleaning code`

Never commit data files (parquet, CSV). Ensure `.gitignore` covers the output path.

## Translation

All descriptions and names must be provided in Portuguese, English, and Spanish. When only one language is available (typically Portuguese from architecture tables), translate to the other two using domain knowledge of Brazilian public administration and statistics. Apply consistent terminology.

## Architecture table is the source of truth

When there is a conflict between raw data column names, DBT file conventions, and the architecture table, the architecture table wins. Update the other artifacts to match.
