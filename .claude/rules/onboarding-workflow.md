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
9b. publish dev/staging  flip the dev/staging dataset under_review → published (NOT the public prod frontend) so the reviewer sees it as it will appear
[PAUSE — verification checkpoint]
10. metadata --env prod  promote to prod (dataset status = under_review; only after human approval)
11. pr                   open PR with changelog
12. pipeline             recurring sources only — add a Prefect refresh pipeline
[PR MERGES + GH table-approve action runs + prod tables verified]
13. publish              flip the prod dataset status under_review → published
14. cleanup              delete the downloaded raw data + cleaned parquet (see below)
```

## Scratch data location and cleanup (steps 3–4, and step 14)

Raw downloads and cleaned parquet **never** go in the repo or anywhere under
Dropbox — that would trigger a multi-GB sync and risk committing data. Put all
intermediate data under **`~/Downloads/<gcp_dataset_id>_data/`** (`input/` for the
downloaded archives, `output/` for the partitioned parquet), and have the
cleaning/upload scripts default to that location (overridable via an env var).
For very large sources, download and clean **one partition at a time and delete
each archive after cleaning** so peak disk stays near a single file.

**Step 14 — delete it all as the final step**, once everything else is done (data
uploaded and verified in prod, PR merged, dataset published). Remove
`~/Downloads/<gcp_dataset_id>_data/` entirely — it is fully reproducible from the
source. When the run stops early (e.g. before the PR, by request), still delete
the scratch data as the last action of that run unless told to keep it.

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
show `dbt run OK` + `dbt test OK` for every table. Three traps around that run:

- The PR needs the **`deploy-flow` label** or the staging deploy is `skipped` and nothing
  is deployed — silently.
- The label only covers a PR that changes **`flows.py`**. `deploy_flows.py` keeps only
  files defining a `Flow`, so a fix in `utils.py`/`tasks.py`/`*_clean.py` deploys
  **nothing** while the job still reports `pass`, and the trigger then runs whatever
  branch the deployment already pointed at. Confirm the clone path in the logs is
  `/app/pipelines-<your-branch>/`, not `/app/pipelines-main/`. See
  `prefect-pipeline-conventions`.
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

## Dataset status lifecycle (publish dev/staging pre-promotion; prod only post-merge)

**Register every dataset as `status = under_review` in step 9**, at every stage. `under_review` hides the dataset from the **production** frontend, so a dataset whose metadata is registered before its PR lands (and whose prod cloud tables do not yet exist) cannot leak publicly.

**Then, on dev/staging, publish the dataset before the PR/prod-promotion step (step 9b).** Once the dev/staging metadata is registered and the tables verified, flip the **dev/staging** dataset `under_review → published`: `create_update_dataset(id=<dataset_id>, …, status_id=status.published, env=<dev|staging>)` (re-pass every required field — no partial updates). The dev/staging frontend is not the public production site, so publishing there is safe, and it lets the human reviewer see the dataset exactly as it will appear at the verification checkpoint. This is the only place a dataset is published before merge — and only on dev/staging, never prod. When extending an already-published dataset, keep it published and refresh its description if the coverage changed.

Turn the **prod** dataset to `status = published` **only in step 13, and only after all three hold**:

1. the onboarding **PR is merged** to `main`;
2. the GitHub **table-approve action has run successfully** (it materialises `basedosdados.<gcp_dataset_id>.*` via `dbt --target prod`; watch for the phantom-model failure mode where non-model `.sql` in the PR aborts materialisation before any real table builds);
3. the live prod tables **and** metadata are **verified** — row counts match, cloud tables resolve, and `get_dataset(slug, env="prod")` shows the expected shape.

Publishing is one call: `create_update_dataset(id=<dataset_id>, …, status_id=status.published, env="prod")` (re-pass every required field — the API does no partial updates). It is a **separate post-merge action**, independent of the optional recurring-pipeline step (12); never publish inside the onboarding PR, and never publish a dataset whose PR has not merged and materialised. Tables are gated by the dataset's status, so they may remain `published`; flipping the dataset makes everything go live in one step.

## Prod table data — materialised by the merge, never uploaded by hand

**You never upload data to the prod project (`basedosdados`) yourself.** During onboarding the local upload targets **`basedosdados-dev` only** (steps 5, and the `set_datalake_project` dbt macro resolves the `dev` target to `basedosdados-dev`). The **prod table data lands in `basedosdados.<gcp_dataset_id>.*` when the onboarding PR is merged**, via the GitHub **table-approve** action, which runs `dbt --target prod` (that target's `set_datalake_project` reads `basedosdados-staging`). So the sequence is: upload to dev → verify in dev → register prod metadata `under_review` (step 10, cloud tables pointing at the not-yet-existing `basedosdados` tables) → open PR (step 11) → **merge → table-approve materialises the prod tables** → verify → publish (step 13). Do not try to populate `basedosdados` or `basedosdados-staging` from a local machine; local credentials are dev-only, and the merge is the trigger. (Watch the phantom-model failure mode noted above: non-model `.sql` in the PR can abort materialisation before any real table builds.)

## Branch and commit discipline

**Branch names — never the generic `claude/…` prefix.** Name the branch for the work, using a prefix that matches the change (the slug is the `<dataset_id>` whenever there is one):

| Prefix | Use for | Example |
|--------|---------|---------|
| `data/` | onboarding a new dataset (cleaning code, dbt models, metadata) | `data/br_mma_cnuc` |
| `pipeline/` | adding or fixing a recurring Prefect pipeline | `pipeline/br_mf_divida_ativa` |
| `fix/` | bug fix to existing code, models, or data | `fix/br_tse_eleicoes-schema` |
| `docs/` | documentation or `.claude/rules` changes only | `docs/tag-conventions` |

If a tool or environment created a `claude/…` branch, rename it (`git branch -m <new-name>`) before opening the PR — **except** when a recurring-pipeline PR has already registered its dev deployment from that branch (the deploy step pins the deployment's `GitRepository` to the PR branch, so renaming mid-PR breaks the deployment's source). In that one case, keep the branch and use the correct prefix next time.

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
