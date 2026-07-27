# AGENTS.md

This document provides guidance for AI agents working on the `basedosdados/pipelines` repository. It covers project structure, workflows, conventions, and important rules to follow.

## Project Overview

This repository contains:
- **Prefect flows** (`pipelines/datasets/`) — data capture and ingestion pipelines for frequently updated datasets.
- **dbt models** (`models/`) — ELT/ETL transformations that materialize data in BigQuery for less-frequently updated datasets.

The primary datalake target is **Google BigQuery**, accessed via the `basedosdados` Python SDK.

## Environment Details

- Python version: `>=3.10,<3.11`
- Package manager: [`uv`](https://docs.astral.sh/uv/)
- Install dependencies: `uv sync`
- Install pre-commit hooks: `uv run pre-commit install --install-hooks`
- Install dbt packages: `uv run dbt deps`
- dbt version: `dbt-core==1.5.6` with `dbt-bigquery==1.5.9`
- Dockerfile: the production runtime is defined in `Dockerfile`. It installs system dependencies. When debugging missing system libraries, check the Dockerfile first — native dependencies must be added there, not in `pyproject.toml`.

<!-- Add env vars description? -->

## Repository Structure

```
pipelines/
├── pipelines/
│   └── datasets/             # One directory per dataset (Prefect flows)
│       └── <dataset_id>/
│           ├── __init__.py
│           ├── constants.py  # Dataset-level constants
│           ├── flows.py      # Prefect flow definitions (+ inline schedule)
│           ├── tasks.py      # Prefect task definitions
│           └── utils.py      # Helper functions
├── models/                   # dbt models (one dir per dataset)
│   └── <dataset_id>/
│       ├── <dataset_id>__<table_id>.sql
│       └── schema.yml
├── macros/                   # dbt macros
├── tests-dbt/generic/        # Custom generic dbt tests
├── dbt_project.yml
├── manage.py                 # CLI for creating/listing pipelines
├── pyproject.toml
└── profiles.yml
```

> [!IMPORTANT]
> This project uses **Prefect 3** (`prefect>=3.0,<4`, pinned in `pyproject.toml`). Write flows and tasks with the `@flow` / `@task` decorators and refer to the [Prefect 3 docs](https://docs.prefect.io/). Do **not** use Prefect 1.x (`0.15.x`) patterns — `with Flow(...) as flow:`, `schedules.py`, `run_config`, `storage`, `max_retries=` / `retry_delay=`. The migration is complete: no dataset flow still uses them. Note that `deploy_flows.py` silently *skips* any file that fails to import, so a flow written against the old API is not deployed rather than reported as an error.

## Working with Prefect Pipelines

### Creating a new pipeline

Use `manage.py` to scaffold a new pipeline from the template:

```sh
uv run manage.py add-pipeline <dataset_id>
```

- `<dataset_id>` must be in **snake_case** and must be unique.
- To list existing pipelines: `uv run manage.py list-pipelines`

### File conventions

- `flows.py`: Define flows with `@flow`. Flows **must be defined at module level in this file** — `deploy_flows.py` only collects `Flow` objects whose function is defined there (an `obj.fn.__code__.co_filename` check).
- `tasks.py`: Define tasks with `@task`.
- `constants.py`: Use a `constants` enum or plain constants — no hardcoded values elsewhere.
- `utils.py`: Pure helper functions with no Prefect decorators.

There is no `schedules.py`. Attach the schedule to the flow object in `flows.py`; CI turns
these dicts into `Cron` objects at deploy time:

```python
my_flow.deploy_schedules = [{"cron": "0 16 10 * *", "timezone": "America/Sao_Paulo"}]
my_flow.job_variables = {"memory": "8Gi"}   # optional; size to the flow's peak RAM
```

### Testing locally

A Prefect 3 flow is a plain callable — import it and call it:

```python
from pipelines.datasets.<dataset_id>.flows import my_flow

my_flow(materialize_to_prod=False, update_metadata=False)
```

Run with `uv run python test.py`. Only the pure download/transform half runs locally: the
upload, dbt, and metadata steps need credentials that exist on the deployed worker, so
expect those to fail on a laptop and say so rather than working around it.

### Deploying and testing on the cloud

Flows reach Prefect 3 through CI only — never register storage or run-config by hand:

| | Staging | Production |
|---|---|---|
| Workflow | `cd-prefect3-staging.yaml` | `cd-prefect3.yaml` |
| Trigger | PR to `main` carrying the **`deploy-flow`** label | push to `main` |
| Scope | changed `pipelines/**/*.py` only | `--all` |
| Pool | `basedosdados-dev` — schedules stripped, manual runs only | `basedosdados` — schedules active |

Both deploy with `paused=True`; production deployments are activated by the backend sync
step (`admin-tools/sync-deployments/`), which is soft-failed with `|| echo` and can leave
a deployment paused without failing the job.

To exercise a flow before merging: add the **`deploy-flow`** label to the PR (the workflow
triggers on the `labeled` event), then start a run from the Prefect UI with any
prod-writing parameters disabled.

## Working with dbt models

### Naming convention

SQL files follow the pattern: `<dataset_id>__<table_id>.sql` (double underscore separator).

### The `set_datalake_project` macro

All models must use this macro to reference staging data:

```sql
select col_name
from {{ set_datalake_project("<dataset_id>_staging.<table_id>") }}
```

- **Do not** use `set_datalake_project` for joins. Joins must reference production tables directly: `basedosdados.<dataset_id>.<table_id>`.

### Running models

```sh
# Single model by name
dbt run --select <dataset_id>__<table_id>

# All models in a dataset directory
dbt run --select models/<dataset_id>
```

The default `--target` is `dev`, which reads from `basedosdados-dev` and writes to `basedosdados-dev`. Do not specify `--target` during local development.

### `schema.yml` and tests

Every model must have a `schema.yml` entry inside its dataset directory. This file declares model metadata and all data quality tests. Custom generic tests live in `tests-dbt/generic/`.

#### Referential integrity

Use `relationships` to validate foreign keys against directory tables:

```yaml
models:
  - name: dataset_id__table_id
    columns:
      - name: id_municipio
        tests:
          - relationships:
              to: ref('br_bd_diretorios_brasil__municipio')
              field: id_municipio
```

#### Unique combinations of columns

```yaml
models:
  - name: dataset_id__table_id
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [col_a, col_b]
```

#### Non-null proportion (custom)

Validates that at least a given proportion of rows are non-null across multiple columns:

```yaml
models:
  - name: dataset_id__table_id
    tests:
      - not_null_proportion_multiple_columns:
          at_least: 0.95
```

#### Custom referential integrity (`custom_relationships`)

Allows ignoring specific values and tolerating a proportion of unmatched rows. **Always document the exceptions in the model description.**

```yaml
models:
  - name: dataset_id__table_id
    description: "Table description. Exception: value '5410' is ignored in id_sh4 relationship test because ..."
    tests:
      - custom_relationships:
          to: ref('br_bd_diretorios_mundo__sistema_harmonizado')
          field: id_sh4
          ignore_values: ['5410']
          proportion_allowed_failures: 0
```

#### Custom unique combinations (`custom_unique_combinations_of_columns`)

Allows a proportion of duplicate key combinations. Use sparingly — it can mask duplicate rows. **Always document the exceptions in the model description.**

```yaml
models:
  - name: dataset_id__table_id
    description: "Table description. Exception: up to 5% duplicate combinations allowed because ..."
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns: [col_a, col_b]
          proportion_allowed_failures: 0.05
```

#### Incremental tests (large tables)

For large tables, scope tests to only the most recent rows using the `where` config. The macro `custom_get_where_subquery` (`macros/custom_get_where_subquery.sql`) detects these keywords and replaces them with the actual most-recent values at runtime.

| Keyword | Columns used |
|---|---|
| `__most_recent_year_month__` | `ano`, `mes` |
| `__most_recent_date__` | `data` |
| `__most_recent_year__` | `ano` |

```yaml
models:
  - name: dataset_id__table_id
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns: [col_a]
          proportion_allowed_failures: 0.05
          config:
            where: __most_recent_year_month__
```

You can also pass a literal SQL expression instead of a keyword:

```yaml
config:
  where: "date_column = '2024-01-01'"
```

### Running tests

```sh
dbt test --select <dataset_id>__<table_id>
dbt test --select models/<dataset_id>
```

## Code Style

- Linter: **Ruff** (`uv run ruff check .`) — line length 79, Python 3.10 target.
- SQL formatter: **sqlfmt** (`uv run sqlfmt .`) — excludes `target/`, `dbt_packages/`, `.venv/`.
- YAML formatter: **yamlfix**.
- Pre-commit hooks enforce all of the above automatically on commit.
- Never bypass hooks with `--no-verify`.
- Add type hints and docstrings for python functions following Google Style.

## Dataset Onboarding

To onboard a new dataset (raw data → BigQuery → metadata), spawn the `orchestrator` agent:

```text
Onboard dataset <slug>.
Sources: <URL or local path to raw files>
Drive folder: BD/Dados/Conjuntos/<slug>/
Architecture suggestion: <brief description>
Organization: <source org name>
Notes: <anything unusual>
```

The agent runs an 11-step sequence: context → architecture → download → clean → upload → dbt → validate → discover → metadata (dev) → [human approval] → metadata (prod) → PR.

### Multi-agent architecture

The `orchestrator` agent dispatches to specialized worker agents:

| Agent | Role |
|-------|------|
| `context` | Gathers source URLs, org, themes, coverage |
| `architecture` | Designs/validates architecture tables on Drive |
| `raw-data-downloader` | Downloads raw files from source URLs or portals |
| `cleaner` | Writes and runs Python cleaning code → Parquet |
| `uploader` | Uploads Parquet to BigQuery |
| `dbt` | Writes SQL models and schema.yml |
| `validator` | Runs DBT tests; flags and fixes failures |
| `discover` | Resolves reference IDs from the backend |
| `metadata` | Registers/updates all metadata in the backend |
| `pr` | Opens the GitHub pull request |

### Rules reference

Agents use shared rule files in `.claude/rules/`:

| Rule file | Contents |
|-----------|----------|
| `data-basis-style.md` | Column naming, ordering, prefixes, directory mappings |
| `dbt-conventions.md` | SQL patterns, schema.yml structure, test types |
| `bigquery-conventions.md` | Project references, partitioning, type casting |
| `metadata-schema.md` | Backend API field mapping, MCP tool sequence |
| `onboarding-workflow.md` | 11-step sequence, quality gates, commit discipline |

### Skills (user-callable shortcuts)

Individual steps can also be invoked directly as skills (`.claude/skills/`):
`/onboarding-context`, `/onboarding-architecture`, `/onboarding-download`,
`/onboarding-clean`, `/onboarding-upload`, `/onboarding-dbt`, `/onboarding-validate`,
`/onboarding-discover`, `/onboarding-metadata`, `/onboarding-pr`

### Prerequisites

Before running AI-assisted onboarding, ensure the following are configured:

1. **`mcp__databasis` MCP server** — Data Basis backend API. Required by `discover` and `metadata` agents.
2. **`mcp__databasis-workspace` MCP server** — Google Drive/Sheets access via `rdahis@basedosdados.org`. Required by `context`, `architecture`, and `metadata` agents to read and write architecture tables on Drive.
3. **`mcp__github` MCP server** — GitHub API. Required by the `pr` agent.
4. **`~/.basedosdados/config.toml`** — basedosdados SDK config. Required by the `uploader` agent (`basedosdados config init`).
5. **`GOOGLE_APPLICATION_CREDENTIALS`** — Service account key with BigQuery write access to `basedosdados-dev`.

## Key Rules for Agents

1. **Never hardcode credentials or secrets.** Use environment variables or Vault.
2. **Always use `set_datalake_project` macro** in model SQL files, except for joins which must use production project references.
3. **Follow snake_case** for all dataset/pipeline names.
4. **Run `uv run pre-commit run --all-files`** after making changes to verify formatting and linting before committing.
5. **Do not modify `dbt_packages/` or `target/`** — these are generated directories.
6. **Do not create a `test.py` file with real credentials** — it is gitignored and for local use only.
7. **Document exceptions** in `schema.yml` model descriptions when using `custom_relationships` or `custom_unique_combinations_of_columns` with non-zero `proportion_allowed_failures`.
8. When adding a new dataset pipeline, always run `uv run manage.py add-pipeline <name>` rather than creating files manually.
9. The `dbt` CLI must be run inside the activated virtual environment: `source .venv/bin/activate` or via `uv run dbt ...`.
