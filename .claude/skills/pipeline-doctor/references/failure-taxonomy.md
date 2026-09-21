# Failure Taxonomy

Every signature below was observed in `list_flow_runs(state="Failed")` on this
repo in August 2026. Match on the signature, then confirm the cause against the
code or the source before proposing anything.

The order matters: classes 7–9 are **not fixable in this repo**, and patching
around one produces a change that looks like a fix and is not.

---

## 1. Source schema change

**Signatures**

```
KeyError: 'Año'                     (mx_sesnsp_incidencia_delictiva)
KeyError: 'id_senador'              (br_senado_dados_abertos)
KeyError: 'naturezas'               (br_rf_cnpj__dicionario)
ArrowTypeError: Unable to merge: Field year has incompatible types:
  string vs dictionary<values=int32, ...>          (us_fec_campaign_finance)
```

**Cause.** The publisher renamed, dropped, retyped or reordered a column. A
`KeyError` naming a source-side column is nearly always this. An `ArrowTypeError`
on a concat/merge means two vintages of the source now disagree on a column's
type.

**Confirm.** Download the current file and diff its header against the column
list in `pipelines/datasets/<ds>/constants.py` and the architecture CSV under
`models/<ds>/code/architecture/`. Name the old and new column explicitly.

**Fix.** Map the new name in the cleaning transform in
`pipelines/datasets/<ds>/utils.py` — the transform is shared with the
`models/<ds>/code/` bootstrap, so fix it once there and do not fork it. Keep the
architecture CSV authoritative: if the *output* schema must change, the
architecture changes first and the dbt model follows. For a retype, normalize
in the transform; staging is all-STRING by convention and the dbt model
`safe_cast`s.

**Watch.** `safe_cast` NULLs instead of raising, so a column silently arriving
empty passes every test. After a rename fix, diff non-null counts against the
staging parquet rather than trusting green tests.

---

## 2. Source unavailable, moved, or throttled

**Signatures**

```
FileNotFoundError: Nenhum arquivo csv encontrado em /tmp/br_ms_sia/output/...
FileNotFoundError: [Errno 2] No such file: '/tmp/br_sfb_sicar_.../RO_AREA_IMOVEL.zip'
ValueError: Não há arquivos disponíveis para a data 2028-08-01.
  Verifique o FTP da Receita Federal.          (br_rf_cafir)
RuntimeError: GET https://legis.senado.leg.br/... (xml) failed:
  HTTP 200 after 6 attempts                    (br_senado_dados_abertos)
```

**Cause, three distinct ones — separate them.**

- *Genuinely absent upstream*: the publisher has not released, or withdrew a file.
- *Download silently produced nothing*: the fetch "succeeded" and the extract
  step found an empty directory. A `FileNotFoundError` in an `output/` path is
  this, not an upstream outage.
- *A date the pipeline computed itself*: `2028-08-01` in a 2026 run is a
  date-derivation bug in our code, not a missing upstream file. Read the error
  before believing it.

`HTTP 200 after 6 attempts` means the request succeeded but the body failed to
parse — an HTML error page or an empty document served with a 200. Treat it as a
source-contract change, not a network fault.

**Fix.** For a real outage, make the poll guard hold instead of failing. For the
other two, fix the code. Never make a pipeline swallow a missing file: a run that
completes on no data is worse than one that fails.

---

## 3. dbt test failure

**Signature**

```
Exception: dbt test falhou para models/<ds>/<ds>__<table>.sql (target=dev|prod)
```

**Cause — decide which before touching anything.**

- *Real data defect*: the new vintage genuinely violates the constraint
  (duplicate keys, a broken FK, a column that went null).
- *Stale test*: the constraint encoded an assumption the source has since
  outgrown legitimately.
- *Ordering artifact*: a cross-table test (`relationships`, `dbt_utils`
  referential checks, `custom_dictionary_coverage` with `ref('..._dicionario')`)
  ran before its sibling model was built. `Not found: Table ....dicionario` is
  this. The flow must run **every** table then test **every** table — two loops,
  never `dbt_command="run/test"` inside a per-table loop.

**Confirm.** Get the failing test's name and row count from the run logs, then
reproduce locally: `uv run dbt test --select <ds>__<table>` (target `dev` is the
default — never pass `--target dev`).

**Fix.** A real defect is fixed in the transform. Relaxing a test is a last
resort and must be documented in the model description, per repo convention
(`custom_relationships` with `ignore_values`, or
`custom_unique_combinations_of_columns` with `proportion_allowed_failures`).
Never relax a test to make a run go green without saying why in the PR.

**Cost note.** `not_null_proportion_multiple_columns` compiles a scan of *every*
column. Unscoped on a wide table it burns the daily BigQuery byte quota — which
then fails whichever pipeline runs next. Scope it with `where:
__most_recent_year_month__` (or the year/date variant).

---

## 4. dbt run failure

**Signatures**

```
Exception: dbt run falhou para models/<ds>/<ds>__<table>.sql (target=dev|prod)
GenericGBQException: Reason: 400 ... Unrecognized name: cycle at [3:18]
Parquet column '<col>' has type BYTE_ARRAY which does not match the
  target cpp_type INT32
Invalid cast from INT64 to DATE
```

**Cause.** The model and the staging external table disagree. `Unrecognized
name` = the model selects a column staging no longer has (upstream rename, or a
transform change that did not reach the model).

The parquet type errors are the known staging-schema divergence: staging must be
**all-STRING** on both write paths — the one-shot onboarding upload *and* the
pipeline's `upload_to_gcs`. They share one staging dataset, so a typed external
table left behind by onboarding collides with the pipeline's all-STRING
overwrite. Cast via arrow, never `astype(str)` (which writes the literal
`"nan"`), and pass the architecture's real types through first so `year`
serializes as `"1959"`, not `"1959.0"`.

**Fix.** Realign model, transform and architecture — architecture wins on any
conflict. Get the real dbt error from the run logs; the wrapper exception text
alone does not identify the column.

---

## 5. Coverage / metadata misconfiguration

**Signature**

```
ValidationError: 1 validation error for PartBdpro
  Value error, date_column 'date' incompatível com date_format '%Y-%m'
                                              (br_rf_cnpj__estabelecimentos)
part_bdpro exige Coverage free + pro          (assert_coverage_topology)
allRawdatasource: mais de um nó encontrado para {'tables_Id': …}
```

**Cause.**

- The `CoverageSpec`'s `date_column` kind does not match its `date_format`:
  `YearMonth` ↔ `YEAR_MONTH`, `YearOnly` ↔ `YEAR`, `DateOnly` ↔ `YEAR_MD`.
- `assert_coverage_topology` hard-fails **before any write** when the tier and
  the registered Coverages disagree. `part_bdpro` needs both a free Coverage
  (`is_closed=False`) and a pro one (`is_closed=True`); `AllFree` needs free and
  no pro. Create the missing Coverage before switching a tier.
- `mais de um nó` is the known client bug: a table linked to two raw data
  sources cannot resolve one, and both the poll and commit tasks go through that
  resolver. Link exactly **one** raw source per table.

**Fix.** These are pure-function or metadata fixes. `assert_coverage_topology`,
`compute_coverage_ranges` and `needs_row_access_policy` are unit-testable
locally — test the window and its roll. `apply_row_access_policies` issues real
BigQuery DDL and is not exercisable locally; say so.

---

## 6. Pipeline code bug

**Signatures**

```
IndexError: list index out of range     (us_bls_oes, br_bndes_operacoes_...)
```

**Cause.** Usually a parse step assuming a structure the current payload does
not have — an empty result set, a missing table on a scraped page, an
off-by-one in a split. Frequently a *downstream* symptom of class 1 or 2.

**Confirm.** Pull full logs (`get_flow_run_logs`, `min_level="ERROR"` first,
then unfiltered around the failure) and read the traceback's own frame — the
`state_message` shows only the outermost exception.

**Fix.** In `utils.py`, with the pure function tested against the real payload.
Fail loudly on an empty payload rather than indexing into it.

---

## 7. IAM / permissions — NOT fixable here

**Signatures**

```
Forbidden: 403 PATCH .../projects/basedosdados-staging/datasets/<ds>_staging/
  tables/<t>: Permission bigquery.tables.update denied
Forbidden: 403 GET .../projects/basedosdados-dev/...:
  Permission bigquery.tables.get denied
Forbidden: 403 GET .../storage/v1/b/basedosdados-dev/o?...:
  Caller does not have serviceusage.services.use access
```

**Cause.** The worker's service account lacks a grant on a project, dataset or
bucket. Nothing in this repo grants it.

**Recognise the shape.** This class arrives as a *cluster* — one missing grant
breaks every pipeline touching that project. In the August 2026 survey it hit
`br_anatel_telefonia_movel`, `br_ms_sih`, `us_treasury_usaspending` and
`br_sfb_sicar` simultaneously. Report it once, with the full list of affected
flows and the exact permission and resource, not once per dataset.

**Do not** work around it by retargeting a bucket or project. That changes where
data lands to make an error disappear.

---

## 8. Worker / orchestration — NOT fixable here

**Signature**

```
UnfinishedRun: Run is in PENDING state, its result is not available.
  (flow "BD template: Executa DBT model")
```

**Cause.** A subflow or task never left `PENDING` — worker capacity, a pod
eviction, or an infrastructure hiccup. Not a data or code fault.

**Fix.** Re-run once to distinguish transient from persistent. If it recurs on
the same deployment, escalate with the run IDs; do not restructure a flow to
route around the orchestrator.

---

## 9. BigQuery quota exhaustion

**Signature.** Quota/`Exceeded` errors, or a cluster of unrelated pipelines
failing within the same window.

**Cause.** Pipelines share a **daily byte quota**. It trips on whichever
pipeline runs next, not the one responsible — so the failing flow is usually
innocent.

**Confirm.** On the `feat/pipeline-diagnostics` branch,
`uv run python -m pipelines.diagnostics cost --days 7` ranks datasets by bytes
billed. It needs `bigquery.jobs.listAll` on the project
(`roles/bigquery.resourceViewer`); a 403 there is a permissions gap, not a
broken query.

**Fix.** Reduce work at the actual culprit: scope wide-table tests with
`where:`, drop redundant dev materializations, make heavy models incremental.
Spreading cron minutes reduces same-instant contention but **not** bytes billed
per day — do not present it as a quota fix.

**Related.** Cron minutes are a real, separate problem: twelve pipelines added
in August 2026 all fired at `0 16 * * *`. Pick an unused minute, never `0`:

```bash
grep -rho '"cron": "[^"]*"' pipelines/datasets/*/flows.py | sort | uniq -c | sort -rn
```
