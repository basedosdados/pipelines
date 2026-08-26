# Fix and Ship

How a diagnosed fix reaches a green prod run. Every step here exists because
skipping it produces a fix that *looks* shipped and is not.

---

## The labels, and what each actually does

| Label | Workflow | Triggers on | Scope |
|---|---|---|---|
| `deploy-flow` | `cd-prefect3-staging.yaml` | PR labelled or synchronized | Deploys changed `pipelines/**/*.py` to the **`basedosdados-dev`** pool, schedules stripped |
| `test-dev-model` | `ci.yaml` | PR labelled | Runs dbt on changed `models/**/*.sql` against `basedosdados-dev` |
| `table-approve` | `table-approve.yaml` | On merge | Materializes the changed models into prod |
| `check-metadata` | — | PR labelled | Validates metadata between BigQuery and the prod API |
| `deploy-flow-fusion` | — | PR labelled | Same as `deploy-flow` with `DBT_ENGINE=fusion` |

There is **no `trigger-run` label.** A dev run is started through
`mcp__databasis__run_deployment`, not from GitHub.

Pick by what the PR changes: `flows.py` → `deploy-flow`; `models/**/*.sql` →
`test-dev-model`; both → both.

### `deploy-flow` only deploys a PR that changes `flows.py`

The workflow passes changed `pipelines/**/*.py` to `deploy_flows.py --files`,
which keeps only files that **define a `Flow` object**. A PR fixing `utils.py`,
`tasks.py`, `constants.py` or a `*_clean.py` — where pipeline bugs usually live
— contributes **zero** deployments, and the job still reports **`pass`**:

```
senado_api.py:   0 flow(s)
senado_clean.py: 0 flow(s)
flows.py:        1 flow(s)
```

The deployment keeps whatever git ref it already had, so a trigger then runs a
**different branch** — whichever PR last touched that `flows.py`, or `main`.
This is not hypothetical: on `mx_sesnsp_incidencia_delictiva` (#1873) and
`br_senado_dados_abertos` (#1874), three trigger attempts ran another branch and
reproduced the original error, reading exactly like "the fix does not work".

`load_flows_from_file` also **swallows import errors** and returns `{}`, so a
broken import is a silent no-deploy with a green check.

**A green "deploy flows" job is never evidence that anything deployed.**

To validate a non-`flows.py` fix, pick one:
- Touch `flows.py` in the same PR (a real change, not whitespace).
- Merge it and watch the next scheduled run — usually fine when the flow is
  already failing every run, so the fix cannot make things worse.

---

## Triggering the dev run

Confirm `cd-prefect3 (staging)` logged `registrado`, then:

```
mcp__databasis__run_deployment(
    deployment_name="<flow_name>/<deployment_name>",
    parameters={"materialize_to_prod": False,
                "update_metadata": False,
                "force_run": True},
)
```

**All three parameters matter.** The flow defaults are
`materialize_to_prod=True, update_metadata=True, force_run=False`. A run
triggered with `{}` writes **prod data, prod metadata, and applies the paywall**
— from the dev pool, because the metadata tasks are pinned `env="prod"`
regardless of pool. `force_run=True` bypasses the poll guard, which otherwise
returns before doing anything.

Get the exact deployment name from
`scripts/deployments.py` (the `DEPLOYMENT` column) — `run_deployment` wants
`<flow>/<deployment>` and must contain exactly one `/`.

### Reading the result

**Done means all four:**

1. Every task `COMPLETED`.
2. `dbt run OK` in the logs for **every** table.
3. `dbt test OK` in the logs for **every** table.
4. The clone path in the logs is `/app/pipelines-<your-branch>/`, not
   `/app/pipelines-main/`.

`COMPLETED` on its own proves nothing: the poll guard returns early and still
completes. `Não há novas atualizações na fonte original` in the logs means the
run polled and did nothing — with `force_run=True` you should not see it.

Read logs with `mcp__databasis__get_flow_run_logs(<run_id>)`. Start at
`min_level="ERROR"`, then re-read unfiltered around the failure — the outermost
exception in `state_message` usually names the wrapper, not the cause.

---

## Reading the stored arming state

`scripts/deployments.py` reports Prefect's `paused`. The backend also stores
`is_schedule_active` and `reactivated_at` in `DisabledFlowSchedule`, and that
pair is what `sync-deployments` re-enforces on every merge to `main`.

To read the backend record for a **paused** deployment without changing
anything:

```
mcp__databasis__set_deployment_schedule_active(
    flow_name="<bare deployment name>", active=False, env="prod")
```

Setting the state a flow is already in is a no-op (`action="no_change"`) that
never touches Prefect, and returns `is_schedule_active` and `reactivated_at`.
Passing `active=False` can only ever pause, so on an already-paused deployment
it is safe as a read.

**Never call it with `active=True` to "read" state.** On a deployment that is
paused it would arm it, and the first armed run is the first-ever execution of
the prod upload — and, for a `part_bdpro` table, of the BigQuery Row Access
Policies.

`flow_name` here is Prefect's **bare deployment name**
(`au_rba_statistical_tables_flow`), not `<flow>/<deployment>` — the same string
in the Django admin's `flow_name` column.

---

## Re-arming: always an explicit human decision

Merging does **not** arm anything. Prod deployments land `paused=True`, and the
backend sync registers an unknown deployment with `is_schedule_active=False` —
sync only *enforces* stored state for deployments it already knows; it never
arms a new one.

Arming is three writes together (stored flag, `reactivated_at`, Prefect
unpause), which `set_deployment_schedule_active(..., active=True)` does in one
call. Do not unpause through the Prefect API directly: `sync-deployments` will
silently re-pause it at the next unrelated merge.

**Ask before arming, every time.** A pipeline the team disarmed was disarmed on
purpose. Re-arming resumes writes to production, and for a `part_bdpro` table
re-issues Row Access Policies — the paywall goes live and the open-data export
starts excluding the pro window. Present the case, name what the first run will
do, and let the user decide. Watch that run.

---

## Branch, commit, PR

Branch prefix by change type — never a generic `claude/…`:

| Prefix | For |
|---|---|
| `fix/` | a bug in existing code, models or data |
| `pipeline/` | adding or restructuring a recurring pipeline |
| `data/` | onboarding a new dataset |
| `docs/` | documentation or `.claude/rules` only |

One exception: if a pipeline PR already registered a dev deployment from its
branch, do **not** rename it — the deployment's `GitRepository` is pinned to
that branch name.

Before committing:

```bash
uv run pre-commit run --files <changed files>
uv run pyrefly check
```

`pyrefly` is a CI job that blocks merge on any error, and the pre-commit hook
for it is skipped on the hosted runner — so run it yourself. Never bypass hooks
with `--no-verify`.

Commit as `fix(<dataset_id>): <description>`. Never commit data (`input/`,
`output/`, parquet, CSV).

Keep framework changes — `pipelines/utils`, `AGENTS.md`, `.claude/rules` — out
of a dataset fix PR. They change behaviour for every pipeline and deserve their
own review.

### PR body

State the diagnosis, not just the change:

- The failing runs (flow name, run IDs, dates) and the error signature.
- The confirmed cause, and how it was confirmed.
- The fix, and why it is the minimal one.
- The dev-run evidence: run ID, and that `dbt run OK` + `dbt test OK` appeared
  for every table, plus the clone path.
- What remains unexercised — the prod upload always, and
  `apply_row_access_policies` when a table is `part_bdpro`.

---

## After merge

`table-approve` materializes prod on merge, but **only the models in the diff**.
A fix that changes a shared macro or a transform without touching the affected
`.sql` will not re-materialize those tables — rematerialize them deliberately
via `run_deployment` on the prod pool.

Watch for the phantom-model failure: non-model `.sql` in the PR
(`macros/`, `tests-dbt/`) makes table-approve treat it as a model and abort
materialization before any real table builds.

Then confirm the pipeline is actually ingesting again — not merely green.
Re-check after the next scheduled run: run state, plus either the log markers or
whether the coverage `DateTimeRange` moved.
