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
│           ├── flows.py      # Prefect flow definitions
│           ├── schedules.py  # Prefect schedule definitions
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
> This project uses **Prefect v0.15.9**, which is a very old version of Prefect 1.x. The API is completely different from Prefect 2.x/3.x. Do not use Prefect 2/3 patterns or documentation. Always refer to the [Prefect 0.15.x docs](https://docs-v1.prefect.io/) and the existing code in `pipelines/datasets/` as reference.

## Working with Prefect Pipelines

### Creating a new pipeline

Use `manage.py` to scaffold a new pipeline from the template:

```sh
uv run manage.py add-pipeline <dataset_id>
```

- `<dataset_id>` must be in **snake_case** and must be unique.
- To list existing pipelines: `uv run manage.py list-pipelines`

### File conventions

- `flows.py`: Define Prefect `Flow` objects. Each flow must be imported in the parent `__init__.py`.
- `tasks.py`: Define Prefect `Task` objects.
- `schedules.py`: Define `Schedule` objects linked to flows.
- `constants.py`: Use a `Constants` enum or plain constants — no hardcoded values elsewhere.
- `utils.py`: Pure helper functions with no Prefect decorators.

### Testing locally

Create a `test.py` at the repo root:

```python
from pipelines.datasets.<dataset_id>.flows import flow
from pipelines.utils.utils import run_local

run_local(flow, parameters={"param": "val"})
```

Run with: `uv run test.py`

### Testing on the cloud

1. Copy `.env.example` to `.env` and fill in `GOOGLE_APPLICATION_CREDENTIALS` and `VAULT_TOKEN`.
2. Load variables: `source .env`
3. Ensure `~/.prefect/auth.toml` exists with `api_key` and `tenant_id`.
4. Create `test.py` using `run_cloud` and run with `uv run test.py`.

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
