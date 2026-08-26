---
name: pipeline-doctor
description: Triage, diagnose, and fix failing Prefect 3 pipelines in this repo. Surveys recent failed flow runs and deployment arming state, groups failures by root cause, classifies each against a failure taxonomy, and drives fixes through branch → PR → label → dev run → merge. Use when asked to check on broken pipelines, find out why a dataset stopped updating, review pipeline health, or fix a failing flow.
argument-hint: "[<flow_name_substring>] [--days N] [--report-only | --fix] [--include-paused]"
---

# Pipeline Doctor

Recurring pipelines in this repo fail for a handful of recurring reasons, and the
team's capacity valve is to **disarm** a pipeline that keeps failing rather than
fix it. Two consequences shape everything below:

- **A quiet pipeline is not a healthy pipeline.** A disarmed deployment produces
  no runs, so it disappears from any failure list. Absence of failures is
  evidence of nothing. Always read arming state alongside run state.
- **A green run is not an ingest.** The source-poll guard returns early and
  Prefect still records `COMPLETED`. `br_ibge_ipca` sat at 4 ingests across 60
  completed runs unnoticed.

The goal is successfully running flows in **prod**. Diagnosis is the means.

Read `references/failure-taxonomy.md` before classifying anything, and
`references/fix-and-ship.md` before opening a PR or triggering a run.

---

## Phase 1 — Survey

Gather three views. Do not skip the second: it is the one nothing else reports.

**Failed runs** (Prefect 3, via the databasis MCP — never the retired
`prefect.basedosdados.org` host):

```
mcp__databasis__list_flow_runs(state="Failed", limit=100)
mcp__databasis__list_flow_runs(state="Crashed", limit=50)
```

`state_message` alone classifies most failures — pull full logs only for the
ones it does not.

**Arming state** — no MCP tool enumerates deployments, so use the bundled
script:

```bash
uv run python .claude/skills/pipeline-doctor/scripts/deployments.py --pool basedosdados --paused
```

Read the `paused` field, never the schedule's own `active` flag: a paused
deployment still reports `active=True`, and deployment-level `paused` wins.
A paused deployment **that carries a cron** was armed once and deliberately
switched off — that is the deactivation signal. A paused deployment with no
cron is usually a helper or a never-armed new pipeline, not a casualty.

Note `updated` is stamped by the last `--all` deploy, i.e. the last merge to
`main`. It is **not** when the pipeline was deactivated and must never be
reported as such. For the real timestamp, see "Reading the stored arming
state" in `references/fix-and-ship.md`.

**Work already in flight — mandatory, and do it before diagnosing anything.**
This is a live repo with many collaborators. Someone else is very likely already
on the failure you are looking at, and a duplicate branch wastes their review
time as well as yours.

```bash
gh pr list --repo basedosdados/pipelines --state open --limit 100 \
  --json number,title,headRefName,labels,updatedAt,author \
  --jq '.[] | "\(.number)\t\(.headRefName)\t\(.title)"'
```

Check **merged** PRs too, not only open ones:

```bash
gh pr list --repo basedosdados/pipelines --state merged --limit 60 \
  --search "sort:updated-desc" \
  --json number,title,headRefName,mergedAt \
  --jq '.[] | "\(.number)\t\(.mergedAt)\t\(.title)"'
```

A merged fix whose merge time is **after** the cluster's last failure means the
cluster is already fixed and the flow is simply not running again — usually
because it was disarmed while it was failing and nobody re-armed it. That is not
a bug to fix; it is an arming decision. `mx_sesnsp_incidencia_delictiva` was
exactly this: its fix merged seven minutes after its final failure, and it sat
paused for days looking broken.

So for every cluster, compare three timestamps before calling it a bug: the last
failure, the merge time of any related PR, and the deployment's arming state.

**Ingest rate** (optional, only on the `feat/pipeline-diagnostics` branch —
`pipelines/diagnostics/` is not on `main` yet):

```bash
uv run python -m pipelines.diagnostics health --days 30
```

It classifies each run from its log markers and flags flows that have never
ingested despite running. If the module is absent, say so rather than
substituting run state for ingest rate.

## Phase 2 — Group by root cause, not by flow

One infrastructure fault shows up as N broken pipelines. Cluster the failures
by their error signature *before* diagnosing, or you will write four PRs for
one IAM grant.

Then cross the clusters against the in-flight and merged PRs from Phase 1, and
against arming state. A cluster that is covered by an open PR, or already fixed
by a merged one, never reaches diagnosis.

Cluster first on the taxonomy signature, then note which datasets each cluster
spans. A cluster of one is fine; a cluster of six that all say
`Permission bigquery.tables.update denied` is a single infrastructure item with
one owner and no code change.

## Phase 3 — Diagnose each cluster

Classify against `references/failure-taxonomy.md`, which gives the observed
signature, the real cause, and the fix route for each class. The classes:

| Class | Fixable in this repo? |
|---|---|
| Source schema change | Yes — the most common real bug |
| Source unavailable / moved | Usually yes |
| dbt test failure | Yes — but decide data-bug vs. stale-test first |
| dbt run failure | Yes |
| Coverage / metadata misconfiguration | Yes |
| Pipeline code bug | Yes |
| IAM / permissions | **No** — escalate, do not patch around |
| Worker / orchestration | **No** — escalate |
| Quota exhaustion | Partly — the culprit is usually another pipeline |

Confirm the diagnosis against the code before proposing a fix. `KeyError: 'Año'`
tells you a column vanished; only the source tells you what replaced it. Fetch
the current source and compare against `constants.py` / the architecture CSVs.

Do not report a cause you have not checked. "Probably the source changed" is not
a diagnosis.

## Phase 4 — Triage

Assign every cluster one disposition, and say who owns it:

- **Fix now** — cause understood, change is contained, verifiable in dev.
- **Needs investigation** — cause plausible but unconfirmed; name the next check.
- **Not ours** — IAM, worker, quota. Escalate with the exact grant or resource.
- **Propose deactivation** — repeatedly failing, low value, no cheap fix. This
  is the team's legitimate capacity decision. **Propose it; never disarm a
  pipeline yourself without explicit approval.**
- **Already fixed** — a merged PR post-dates the last failure. No code change.
  The open question is whether the deployment is armed; say so and stop.
- **Already in flight** — an open PR covers it. Name the PR number and its author,
  and do not start a competing branch. If the PR looks stalled or wrong, say so
  as a comment-worthy observation; do not fork it.

In `--report-only` mode (and by default when more than one cluster needs code
changes), stop here and present the board. Ask before fixing at scale — a sweep
across ten datasets is the user's call, not yours.

## Phase 5 — Fix and ship

Follow `references/fix-and-ship.md` exactly. It carries the traps that make a
fix look shipped when it is not. In brief, per fix:

1. Branch `fix/<dataset_id>-<what>` — never a generic `claude/…` prefix.
2. Make the minimal change. Reuse the shared transform; do not fork it.
3. Pre-format: `uv run pre-commit run --files <changed>`; `uv run pyrefly check`.
4. Open the PR and apply the label that matches what changed:
   `deploy-flow` for `flows.py`, `test-dev-model` for `models/**/*.sql`.
   There is no `trigger-run` label — a dev run is triggered through
   `mcp__databasis__run_deployment`, not GitHub.
5. Verify in dev: trigger with **prod explicitly off**
   `{"materialize_to_prod": false, "update_metadata": false, "force_run": true}`.
   The defaults are `True/True/False`; a run triggered with `{}` writes prod
   data, prod metadata, and applies the paywall — from the dev pool.
6. Read the logs, not the state. Confirm `dbt run OK` **and** `dbt test OK` per
   table, and that the clone path is `/app/pipelines-<your-branch>/`, not
   `/app/pipelines-main/`.
7. Hand the PR to the user to merge. Re-arming a disarmed pipeline is a separate,
   explicit decision — see `references/fix-and-ship.md`.

## Phase 6 — Report

Report in this shape, ranked by blast radius (datasets affected × whether prod
data is stale):

```text
=== PIPELINE DOCTOR — <window>, prod pool ===
Runs surveyed: N failed, M crashed | Deployments: A armed, P paused (C with a cron)

CLUSTER 1 — <class>: <signature>
  Flows:      <flow> (n failures, last <date>), ...
  Cause:      <confirmed cause, and how it was confirmed>
  Disposition:<fix now | investigate | not ours | propose deactivation | in flight>
  Action:     <PR #, or the exact escalation>

... one block per cluster ...

DISARMED WITH A CRON (not producing runs, so invisible above): <n>
  <flow> — last failure <date>, cause <class>

NOT VERIFIED: <what this run could not exercise>
```

Always close with what was not verified. The prod upload and, for a
`part_bdpro` table, `apply_row_access_policies` only execute on an armed prod
run — neither is exercisable from a dev run or locally. Say which applies
rather than implying the pipeline is proven end to end.

## Running this periodically

The skill is designed to be re-run on a schedule. To keep successive runs from
re-reporting the same thing:

- **Re-run the open/merged PR check every time** (Phase 1). On a shared repo the
  in-flight set changes between runs more than the failure set does.
- Keep a small state file at `~/.cache/bd-pipeline-doctor/state.json` recording
  each cluster's signature, first-seen date, and disposition. On a later run,
  report a known cluster as `unchanged since <date>` in one line instead of a
  full block, and reserve full blocks for what is new or has changed
  disposition. Never put this state in the repo.
- In an unattended run, stay in `--report-only`. Opening PRs and triggering
  runs are the parts a human should be present for.
