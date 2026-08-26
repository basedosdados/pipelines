---
name: pipeline-doctor
description: Triage, diagnose, and fix failing Prefect 3 pipelines in this repo. Surveys recent failed flow runs and deployment arming state, groups failures by root cause, classifies each against a failure taxonomy, and drives fixes through branch → PR → label → dev run → merge. Use when asked to check on broken pipelines, find out why a dataset stopped updating, review pipeline health, or fix a failing flow.
argument-hint: "[<flow_name_substring>] [--days N] [--report-only | --fix] [--include-paused]"
---

# Pipeline Doctor

Recurring pipelines in this repo fail for a handful of recurring reasons, and the
team's capacity valve is to **disarm** a pipeline that keeps failing rather than
fix it. Three consequences shape everything below:

- **Only what is failing *now* is a finding.** The failed-run list is a log, not
  a worklist: it keeps showing errors that were fixed days ago. A flow whose most
  recent run succeeded is out of scope — filter it out in Phase 2 and never give
  it a cluster block. Report what is broken today, not what once broke.
- **A quiet pipeline is not a healthy pipeline.** A disarmed deployment produces
  no runs, so it disappears from any failure list. Absence of failures is
  evidence of nothing. Always read arming state alongside run state.
- **A green run is not an ingest.** The source-poll guard returns early and
  Prefect still records `COMPLETED`. `br_ibge_ipca` sat at 4 ingests across 60
  completed runs unnoticed.

The last two points cut against the first, and the resolution is not to split the
difference. A green latest run takes a flow **off the failing board** — it is not
currently failing, which is what was asked. What a green run does *not* establish
is that the flow is *ingesting*, or that it wrote anything to **prod**: a run
triggered `materialize_to_prod=False` exits green having only touched
`basedosdados-dev`. Keep the verdicts apart — "is it failing?", "is it doing
work?", and "does prod actually have the data?" — and never let a green answer to
the first stand in for the other two.

Prefect is authoritative for the first question only. For the third, the
authority is BigQuery (`__TABLES__`) and the metadata checker, which is why a
dataset can be absent from every failure list and still be broken from a user's
point of view.

The goal is successfully running flows in **prod**. Diagnosis is the means.

Read `references/failure-taxonomy.md` before classifying anything, and
`references/fix-and-ship.md` before opening a PR or triggering a run.

---

## Phase 1 — Survey

Gather four views. Do not skip the second (arming state — nothing else reports
it) or the third (successful runs — it decides what is even a finding).

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

A merged PR is a **weaker** signal than a later green run, and it is only ever
corroborating evidence. Merges get reverted, fix the wrong table, or land behind
a `flows.py` no-op deploy that never reached the worker. Use the merge to explain
*why* a flow recovered; use the run to decide *whether* it did.

**Successful runs — mandatory, and the filter that decides what is even on the
board.** The target is *currently failing* pipelines. A flow that failed and has
since succeeded is not a finding, however loud its error was.

```
mcp__databasis__list_flow_runs(state="Completed", limit=100)
mcp__databasis__list_flow_runs(flow_name="<flow>", limit=20)   # per candidate flow
```

For every flow that appears in the failed list, find its **most recent run of any
state**. If that run succeeded, the flow is healthy *now* and is out of scope —
drop it before diagnosis, do not open a PR for it, and do not give it a block in
the report. This is not the same test as "a merged PR post-dates the failure": a
merge is evidence of intent, a later green run is evidence of outcome. **Prefer
the green run.** Where the two disagree, believe the run.

Three traps when reading a "successful" run. Check all three — each catches a
different way a green run can be worthless:

- **Check the duration.** A 3–5 second completion is the poll guard returning
  early, not an ingest. That still counts as *not currently failing*, but it is
  not proof the failing code path was re-exercised. Say which you saw.
- **Check it is the same work.** A flow whose per-table deployment was removed,
  or that succeeded only on a narrower parameter set, has not necessarily
  recovered the failing path.
- **Check it reached prod.** Read the run's *parameters*, not just its state. A
  run triggered with `materialize_to_prod=False` (or the equivalent
  `materialize_after_dump=False`) returns before the prod block: it uploads to
  `basedosdados-dev`, runs `--target dev`, and exits green having never written
  a production table. Duration does not catch this — such a run can do fifteen
  minutes of genuine work.

  In the logs, a run that reached prod shows **both** an upload to
  `gs://basedosdados/staging/...` and `dbt run/test ... --target prod`. If you
  see only `basedosdados-dev` and `--target dev`, the run was a dev validation,
  and it is **not** evidence the pipeline is healthy — the prod tables may not
  exist at all.

  This is not hypothetical. On 2026-08-26 `mx_sesnsp_incidencia_delictiva` was
  read as recovered from a green 14-minute run; the run was dev-only, and all
  four of its ongoing prod tables did not exist in BigQuery. The backend was
  serving metadata for tables that had never been built, and the alert came from
  the metadata checker, not from Prefect.

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

**Apply the recovered-flow filter first, before anything else in this phase.**
Drop every flow whose most recent run succeeded **and reached prod**. Run the
filter on *flows*, not on clusters: one signature can span six flows of which
four have recovered, and the cluster survives only for the two that have not. A
cluster with no surviving flow is deleted outright — it is not a finding, and it
does not get a block, a disposition, or a PR.

"Succeeded" here means the full test from Phase 1, not the state alone. A green
**dev-only** run does not clear a flow off the board: it says the code no longer
raises, not that production has data. Where the latest green run never reached
prod, keep the flow and re-file it under the disposition below.

What remains after this filter is the board. Everything else is history.

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
- **Already in flight** — an open PR covers it. Name the PR number and its author,
  and do not start a competing branch. If the PR looks stalled or wrong, say so
  as a comment-worthy observation; do not fork it.
- **Green but never reached prod** — the code is fixed and the latest run passed,
  but that run was dev-only, so prod may hold stale data or no table at all. The
  flow is not failing and there is nothing to fix; what is missing is a prod run.
  Verify against BigQuery — `__TABLES__` for the dataset in `basedosdados` —
  before describing the state, and report the per-table row counts you found
  rather than inferring from the run. Triggering the prod run writes production
  data and, for a `part_bdpro` table, issues Row Access Policies, so **ask
  first**; it is the same class of decision as arming.

There is deliberately **no "already fixed" disposition.** A flow whose latest run
succeeded was removed in Phase 2 and never reaches triage. Do not reintroduce it
as a finding, a block, or a caveat — the reader asked for what is broken now, and
a recovered flow is not that. It appears in the report only as a count on the
`RECOVERED` line.

The one case worth a sentence is a flow that is **fixed, green, and still
disarmed**: there is no bug, but it is also not running. That is an arming
question, so put it under `DISARMED WITH A CRON`, never in a cluster block.

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

**Every cluster block describes a pipeline that is failing right now.** If its
latest run was green, it does not belong here at any length.

```text
=== PIPELINE DOCTOR — <window>, prod pool ===
Runs surveyed: N failed, M crashed | Deployments: A armed, P paused (C with a cron)
Still failing: <n> flows in <k> clusters | Recovered since failing: <r> (not listed)

CLUSTER 1 — <class>: <signature>
  Flows:      <flow> (n failures, last <date>, latest run STILL FAILING), ...
  Cause:      <confirmed cause, and how it was confirmed>
  Disposition:<fix now | investigate | not ours | propose deactivation | in flight>
  Action:     <PR #, or the exact escalation>

... one block per cluster ...

DISARMED WITH A CRON (not producing runs, so invisible above): <n>
  <flow> — last failure <date>, cause <class>, latest run <green|failing>

GREEN BUT NEVER REACHED PROD (also invisible above): <n>
  <flow> — latest run <id> <date> was dev-only; prod tables: <per-table row
           counts from basedosdados.__TABLES__, or "missing">

NOT VERIFIED: <what this run could not exercise>
```

The `Recovered` figure is a **count on one line, with no flow names**. Its job is
to show the failed-run list was read and filtered, not to walk the reader through
problems that no longer exist. Name a recovered flow only if the user asks.

Every flow inside a cluster block carries an explicit "latest run" note, so a
reader can see the currently-failing claim was checked per flow rather than
assumed from the failure list.

Always close with what was not verified. The prod upload and, for a
`part_bdpro` table, `apply_row_access_policies` only execute on an armed prod
run — neither is exercisable from a dev run or locally. Say which applies
rather than implying the pipeline is proven end to end.

## Running this periodically

The skill is designed to be re-run on a schedule. To keep successive runs from
re-reporting the same thing:

- **Re-run the open/merged PR check every time** (Phase 1). On a shared repo the
  in-flight set changes between runs more than the failure set does.
- **Re-run the latest-run check every time too.** Recovery is the most common
  change between two sweeps. A cluster carried in the state file must be
  re-tested against the flow's newest run before it is reported again — and
  dropped, with its state entry deleted, the moment that run is green. Stale
  clusters resurfacing after they recovered is the main failure mode of running
  this on a schedule.
- Keep a small state file at `~/.cache/bd-pipeline-doctor/state.json` recording
  each cluster's signature, first-seen date, and disposition. On a later run,
  report a known cluster as `unchanged since <date>` in one line instead of a
  full block, and reserve full blocks for what is new or has changed
  disposition. Never put this state in the repo.
- In an unattended run, stay in `--report-only`. Opening PRs and triggering
  runs are the parts a human should be present for.
