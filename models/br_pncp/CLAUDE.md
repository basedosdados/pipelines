# br_pncp

Portal Nacional de Contratações Públicas — public procurement across **all three levels
of government** (federal, state, municipal), from the PNCP consulta REST API. This
extends the federal-only `br_cgu_licitacao_contrato` to states and municipalities: in a
one-day sample of `contrato`, 71% of records were municipal, 13% state and 13% federal.

Coverage starts 2021 (Lei 14.133/2021), but adoption ramps steeply — 5.3k contratos in
2021 against 2.02M in 2025.

## Status: backfill RUNNING, five tables (2026-08-28)

Everything except the historical download is done and verified. The backfill is
resumable: chunks are written atomically and skipped when present, so re-running the
same command picks up exactly where it stopped.

Measured throughput after the window-depth fix is ~825 pages/hr, against
~220 before it. Remaining estimate **22-30 hours**, and the spread is
honest rather than decorative: `contratacao` is crossed with 14 modalidades
and has never been harvested, so its page count is the one number here that
is extrapolated rather than measured. `contrato` (~5h) and
`ata_registro_preco` (~5.5h) are sized from real probes.

**Resume with:**

```bash
PNCP_DATA_DIR=~/Downloads/br_pncp_data PNCP_MIN_INTERVAL=0.05 PNCP_WORKERS=3 \
  uv run python models/br_pncp/code/download.py \
  --tables contrato ata_registro_preco contratacao plano_contratacao_anual instrumento_cobranca
```

Do not raise `PNCP_WORKERS` above 3 — see the concurrency note below; 4 measured 2x
slower.

**Harvested so far** (`~/Downloads/br_pncp_data/input`, 441 MB):

| table | state |
|---|---|
| `contrato` | 112 chunks, 2021-01 .. ~2025-09 (windows through 2026-08 remain) |
| `instrumento_cobranca` | complete, 73,202 records, **13 windows still to re-run** |
| `ata_registro_preco`, `contratacao`, `plano_contratacao_anual` | not started |

The 13 `instrumento_cobranca` gaps failed under the page-size bug fixed in
`22edb965d`; they left no chunk files, so the resume command re-fetches exactly them.

**Estimated remaining: ~40 hours** — `contrato` ~10h, `contratacao` ~11h, `pca` ~14h,
`atas` ~3h, gaps ~1.5h. `pca` is ~35% of that and is the obvious scope lever if the
backfill needs to be shorter; older annual plans are already executed.

**Verified in dev, complete:**

| model | staging | materialized | dbt test |
|---|---|---|---|
| `contrato` | 224,555 | 224,555 | 7/7 |
| `instrumento_cobranca` | 73,202 | 59,312 | 5/5 |

`contrato` row counts match the source exactly for the years the backfill completed
(2021 = 5,308; 2022 = 40,645). 35 unit tests pass.

**Next steps after the backfill finishes:** clean + upload the remaining three tables,
rebuild `dicionario` (it derives from the fact tables, so it must be rebuilt last), run
`dbt run` then `dbt test` across all six models, then register metadata in dev — which
needs a `pncp` **organization created first**, as none exists. Stop at the verification
checkpoint before anything touches prod.

## Scope: five tables now, PCA later

`plano_contratacao_anual` is **deferred to a follow-up backfill**, agreed with
the user on 2026-08-28. It costs ~76h of API time by itself, about twice the
other four tables combined, and there is no cheaper route: `/v1/pca/atualizacao`
serves ~40s pages (measured 39.3s for one 500-item page) and the year-keyed
`/v1/pca/` endpoint times out on every `codigoClassificacaoSuperior` tried.

Shipping the other five gets the dataset through dev validation, metadata and
PR in ~40h rather than ~118h.

**Everything for PCA stays in the codebase** -- its `ENDPOINTS` entry,
architecture CSV, `flatten`/`EXPLODE` handling and `dicionario` mapping. Only
the harvest, upload and dbt scope exclude it, via `constants.DEFERRED_TABLES`.

To pick it up later: add `plano_contratacao_anual` back to `FACT_TABLES`,
`ALL_TABLES` and `gen_dbt.TABLES`, re-run `gen_dbt.py`, harvest it, then
register the table. The `TestDeferredTableScope` tests keep the two halves
honest in the meantime.

**Its dbt model is deliberately absent and that is load-bearing.**
table-approve materialises every model in a PR, so one model whose staging
table does not exist aborts the entire prod materialisation, not just its own.

## Tables

| table | grain | partition | backfill endpoint | pipeline endpoint |
|---|---|---|---|---|
| `contratacao` | one contratação | `ano` (int64) | `/v1/contratacoes/publicacao` | `/v1/contratacoes/atualizacao` |
| `contrato` | one contrato **or empenho** | `ano` (int64) | `/v1/contratos` | `/v1/contratos/atualizacao` |
| `ata_registro_preco` | one ata | `ano` (int64) | `/v1/atas/atualizacao` | same |
| `instrumento_cobranca` | one billing instrument in a contract | `ano` (int64) | `/v1/instrumentoscobranca/inclusao` | same |
| `plano_contratacao_anual` | one **item** of an annual plan | `ano` (int64) | `/v1/pca/atualizacao` | same |
| `dicionario` | — | — | derived | derived |

The backfill uses the *publication-date* endpoints for `contratacao` and `contrato` so
each record is filed under the year it was published; the recurring pipeline uses the
*update-date* endpoints so it also picks up amendments to older records.

`ano` is derived from the PNCP publication date, except for
`plano_contratacao_anual` (the plan's reference year, `anoPca`) and
`instrumento_cobranca` (`dataInclusao`).

## Where the code lives

- `pipelines/datasets/br_pncp/` — the canonical home for the pure transform:
  `constants.py` (endpoints, table lists, lookback), `utils.py` (download + cleaning,
  no Prefect imports), `tasks.py` (Prefect wrappers), `flows.py` (the flow and its
  inline schedule).
- `models/br_pncp/code/` — one-shot onboarding front ends over that package:
  `download.py`, `clean.py`, `build_dicionario.py`, `upload.py`, plus
  `architecture/build_architecture.py` and `gen_dbt.py`.
- `models/br_pncp/` — dbt models and `schema.yml`, both generated by `code/gen_dbt.py`
  from the architecture CSVs.

## Refresh cadence

- `12 4 * * *` — daily, 04:12 America/Sao_Paulo.

Each run re-harvests a 10-day trailing window, deliberately wider than the schedule
interval because PNCP backdates amendments.

### Cost: the backfill is one-time, the daily run is not

The historical backfill is ~28 hours because it pulls 5.7 years through an API that
answers a page in 5-8s. That never recurs. Measured cost of a single scheduled run, on
the update-date endpoints the pipeline actually uses (10-day window, 2026-08-17..26):

| table | records in window | pages |
|---|---|---|
| `contrato` | 81,275 | 163 |
| `contratacao` | 72,555 | 146 |
| `ata_registro_preco` | 27,641 | 56 |
| `instrumento_cobranca` | 4,961 | 50 (100/page) |
| `plano_contratacao_anual` | ~53,000 est. | ~107 |
| **total** | | **~520** |

At the measured ~1,300 pages/hour that is **~25 minutes of API time** per run, plus dbt.

### A >10-day outage leaves a permanent gap

This is the failure mode to watch, and it fails **green**. The harvest window is keyed on
*update* date, so a record published 20 days ago and untouched since falls outside a
10-day window: a run after a two-week outage will never fetch it, and no amount of
deduplication recovers a record that was never downloaded.

`br_ibge_ipca` (4 ingests in 60 completed runs) and `br_bcb_estban` (nothing ingested for
months) are the precedents in this repo.

Recovery is a manual trigger with a wider window — the flow takes `lookback_days`:

```
run_deployment(..., parameters={"lookback_days": 90, "force_run": True,
                                "materialize_to_prod": True})
```

After any outage longer than the lookback, run that **before** trusting the next
scheduled run. Widening the default instead is not free: 30 days is ~1,560 pages, roughly
72 minutes per daily run. Overlapping runs are idempotent: staging is
append-only and the dbt models are incremental with `insert_overwrite` on `ano`,
collapsing repeats on the PNCP control number and keeping the latest `data_atualizacao`.

The incremental filter scopes to the partitions a run touched (`pncp_years` var) rather
than to recently-updated rows. That distinction is load-bearing: `insert_overwrite`
replaces a partition wholesale, so filtering on recency would rewrite each year with only
that run's delta and silently drop the rest of it.

## API traps

Every one of these was found by probing; none is stated in the OpenAPI spec.

1. **`/v1/atas` filters on *vigência*, not publication date.** A single day
   (`2025-03-10`) returns **365,742** records in 21s — the entire live stock of atas
   whose term covers that day. Windowing it duplicates the stock once per window. Use
   `/v1/atas/atualizacao` (1,551 records in 1.5s for the same day) and dedupe on
   `numeroControlePNCPAta`. The same reasoning applies to `pca/atualizacao`.

2. **Empenhos are not a separate endpoint.** They arrive inside `/v1/contratos` as
   `tipoContrato = "Empenho"`, around 40% of rows. `contrato` therefore holds both, and
   `id_tipo_contrato` discriminates them. Do not go looking for an empenho endpoint.

3. **`codigoModalidadeContratacao` is mandatory** on both `/contratacoes` endpoints, so
   every date window must be crossed with all 14 modalidades.

3b. **Two endpoints do not behave like the others, and both fail silently or
   confusingly if treated uniformly:**

   - **`instrumentoscobranca/inclusao` caps `tamanhoPagina` at 100**, not 500. Above it
     the response is `400 Tamanho de página inválido`. The cap is per endpoint, so it
     lives in `ENDPOINTS[...]["page_size"]`.
   - **`pca/atualizacao` paginates over *items*, not plans.** Each page returns **one**
     plan record carrying up to `tamanhoPagina` items, and `totalRegistros` counts
     items rather than plans. `totalPaginas` does honour the page size, so paging
     through and exploding on `itens` yields every item exactly once — verified by
     diffing pages 1 and 2 of 2025-03-01..10, which shared **zero** `numeroItem`
     values. Do not "fix" the one-record-per-page response; it is the contract.

4. **Hard caps:** `tamanhoPagina` max 500 on most endpoints (min 10), date window max 365 days (HTTP 422
   beyond). Large result sets make the server fail rather than paginate — a full-year
   window on a high-volume modalidade intermittently returns HTTP 500, and pages deep
   into a big result set return HTTP 504. `download.py` splits any window the server
   refuses, so this is handled without tuning per endpoint.

5. **The API is slow, not tight.** A 500-row page takes **5–8s** to answer, so the
   download is latency-bound and the backfill must be concurrent to finish in reasonable
   time. Measured, with no 429s at any level: 1 worker 8.5s/page, 2 workers 3.7, 3
   workers 3.1, 4 workers 2.7. The harvest runs 4 workers behind a shared pacer.

   **The ceiling is a concurrency limit, not a requests-per-second one**, applied per
   source IP across all endpoints. Measured by issuing extra concurrent requests while a
   4-worker harvest was running: every one came back 429 while the harvest continued at
   200. The 429 body is an HTML F5-style "Limite de Requisições Excedido" page with **no
   `Retry-After`**.

   **Use 3 workers.** 4 sits *on* the ceiling and is materially slower than 3 sitting
   under it, which is counterintuitive enough to be worth stating plainly:

   | workers | pacer in steady state | sustained rate |
   |---|---|---|
   | 4 | spiking to 6.4s | ~590 pages/hour |
   | 3 | flat at 0.05s | ~1,300 pages/hour |

   At 4 workers all four cross the ceiling together, each calls `penalise()`, and the
   compounding (1.6^4) serialises every worker behind a ~6s global pacer. Because the
   real limit is on concurrency, a *rate* penalty is the wrong instrument — it punishes
   all workers for the ceiling being touched. `penalise` is therefore capped at 2.0s, and
   the answer to a 429 is to run fewer workers rather than to back off harder.

   Exceeding it is worse than slow. A 5-worker attempt tripped 429s, which penalised the
   shared pacer up to its 8s ceiling and compounded with the per-window cooldowns; the
   harvest then went ~15 minutes without completing a single window and looked hung.

   The response to a 429 must be to **back off, never to split the window**: splitting
   replaces one refused request with two against the component already complaining. The
   downloader raises a distinct `RateLimitedError` for this reason, and only
   `ServerOverloadError` reaches the splitting path.

5b. **"No results" is signalled three different ways, none of them an error.** A 204;
   a `200` with a **zero-length body** (`pca/atualizacao` on a quiet day); and a **404**
   carrying `{"message": "Nenhum instrumento de Cobrança encontrado."}`. Treating the
   latter two as failures made both endpoints look permanently dead — they are not, and
   an earlier revision of this branch wrongly concluded the two tables could not be
   built at all.

6. **There is no item-level detail for contratações.** `RecuperarCompraPublicacaoDTO`
   carries only free-text `objetoCompra`. Item and catalogue (CPV-like) data exists in
   the *other* PNCP API, one call per contratação — millions of calls, so it is out of
   scope. `plano_contratacao_anual` is the **only** table in this dataset carrying
   catalogue classification (`codigo_item`, `codigo_pdm`,
   `codigo_classificacao_superior`).

## Licence

PNCP's own "Dados Abertos" page returns HTTP 401 (auth-gated). The only licence
statement discoverable on the portal is the gov.br **site-wide footer**, CC BY-ND 3.0.
Taken literally, no-derivatives would forbid republishing transformed data. That footer
is boilerplate on every gov.br portal, including the CGU and ME portals whose data Data
Basis already publishes, and Lei 14.133/2021 art. 174 mandates that PNCP publish
openly. The dataset is onboarded on that precedent, by explicit decision (2026-08-26).
If PNCP ever publishes an explicit data licence, record it here.

## Cleaning notes

- **A few numeric fields are pt-BR formatted strings, not JSON numbers.**
  `notaFiscalEletronica.valorNotaFiscal` arrives as `"4.920,00"` — dot thousands
  separator, comma decimal. `float()` rejects it, so `valor_nota_fiscal` was silently
  100% NULL while every sibling field from the same nested object was 25.3% populated.
  That contrast is the tell worth remembering: when one column of a nested block is empty
  and its neighbours are not, suspect the parse, not the source. `as_number` now
  normalises the pt-BR form.


- Staging parquet is **all-STRING**, cast through arrow (never `astype(str)`, which
  writes the literal `"nan"` for NULL). Real types are applied before the string cast so
  an INT64 year serialises as `"2025"`, not `"2025.0"`.
- The partition column `ano` is written into the **directory name only**, not into the
  parquet file. Both would collide on read (`Field ano has incompatible types: string vs
  dictionary<values=int32>`).
- **Deduplication happens in the dbt model, not in the cleaning step.** PNCP re-delivers
  a record in every harvest window that touched it, so staging holds duplicates by
  design; each model carries an unconditional
  `QUALIFY row_number() over (partition by <pncp control number> order by
  data_atualizacao desc) = 1`. Staging row counts are therefore larger than the
  materialized tables, which is expected, not a loss.

  An earlier version deduplicated in Python with a `{key: row}` dict. That is fine on a
  small table and fatal here — contrato alone is 4.8M rows of 46 string columns, and it
  exhausted the machine's RAM. The cleaning step now streams: rows buffer per partition
  and flush to a numbered parquet part every `PNCP_BATCH_ROWS` (default 50,000), so peak
  memory is bounded by the batch rather than the table. Measured at 443 MB. Several parts
  per `ano=` directory are fine — a hive-partitioned external table reads every file in
  the directory.
- `dicionario` is derived from the cleaned data itself: most PNCP coded fields ship
  their label alongside the code (`modalidadeId` / `modalidadeNome`), so the dictionary
  cannot drift. Only `id_esfera`, `id_poder` and `tipo_pessoa_fornecedor` are
  hard-coded, because the API never labels them.

## Verification so far

The whole chain — download, clean, upload, dbt — has been exercised end to end in dev on
the partial `contrato` backfill (224,555 rows, 2021-09-06 to 2023-11-16), rather than
only at the end:

| check | result |
|---|---|
| row counts vs source | 2021 = 5,308, 2022 = 40,645 — exact match to the API's `totalRegistros` |
| clean peak RSS | 443 MB, bounded by the batch, not the table |
| upload to dev staging | 224,555 rows, 212 MB peak |
| `dbt run` | incremental model created, 126.6 MiB processed |
| materialized types | INTEGER / DATE / FLOAT / BOOLEAN all resolved from the all-STRING staging |
| `safe_cast` loss | zero NULLs in `ano`, `valor_global`, `data_assinatura`, `indicador_receita` |
| dedup key | `count(distinct id_contrato_pncp)` equals the row count |

The row-count match is the load-bearing one: it shows the transform reproduces the
source exactly for every year the backfill has completed, which no amount of green test
output would establish on its own.

Two environment notes. `dbt` needs `--profiles-dir ~/.dbt` locally — the repo's
`profiles.yml` points at a container path (`/credentials-dev/dev.json`). And `dbt parse`
alone takes several minutes across the repo's 1,151 models, so scope every invocation
with `--select br_pncp...`.

### Why staging parts are capped at 50,000 rows

Beyond bounding memory in the cleaning step, this defuses a known repo-wide OOM.
`bd.Table.create` derives the staging header via `_get_columns_from_data`, which takes
`glob("**/*")[0]` — an arbitrary file, not a sorted first — and calls
`pd.read_parquet()` on the whole thing purely to read column names. Other datasets work
around it by prepending a 0-row `00_header.parquet`, which relies on glob order. Capping
every part instead makes the read bounded whichever file is picked.

`pipelines/datasets/br_pncp/tests/` covers the transform's silent-failure modes — NULL
never becoming the literal `"nan"`, integers not acquiring a `.0` tail, the partition
column staying out of the parquet file, PCA exploding to one row per item, and
`replace=False` leaving untouched partitions alone.

## Test scoping

The table-level dbt tests are scoped `where: __most_recent_year__`. Unscoped,
`not_null_proportion_multiple_columns` compiles a scan of every column across every
partition, which on these 40+ column, multi-million-row tables is enough to burn the
BigQuery daily byte quota by itself.

Relationship tests against `br_bd_diretorios_data_tempo__ano` use `field: ano.ano` — the
time directory binds through a STRUCT, and the bare `field: ano` form never passes.

The UF directory's key column is **`sigla`**, not `sigla_uf`. Pointing the foreign key at
`sigla_uf` compiles fine and fails only at run time with `Unrecognized name: sigla_uf`.
The repo's `.claude/rules/data-basis-style.md` documented the wrong column and has been
corrected; every other dataset in the repo already used `field: sigla`.

## Backend IDs resolved (staging, 2026-08-28)

The dataset id `br_pncp` has no organization segment. That is deliberate and
follows `br_jota`, the repo's other two-segment Brazilian dataset: the name is
single-segment when the organization *is* the thing being published. PNCP is a
portal in its own right, so it gets its own organization rather than being filed
under `mgi`.

| Reference | Slug | ID (staging) |
|---|---|---|
| organization | `pncp` | `9c03ec3a-d302-442c-9073-15760370599e` |
| area | `br` | `5503dd29-4d9b-483b-ae09-63dc8ed28875` |
| status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| theme | `government` | `6dd730bb-89ab-4dba-a1bf-a25ca1c35003` |
| theme | `economics` | `ad6a413a-e882-4dd6-a497-8a62eec8511b` |
| license | `cc_by` | `92211312-c1b7-4d21-80c5-fd6715b70e22` |
| availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |

Entities for observation levels: `procurement`
`4cce9a0f-b438-442c-bb94-444445cb1a2d`, `contract`
`38e7435c-f2d1-4ddd-b010-283d0eb77f6c`, `item`
`5713c2f7-70d3-48f9-9b4c-5c531dc467ba`, `year`
`e1bf146e-b6bb-4b65-bee7-c800876e80a5`, `municipality`
`460cf58b-63a7-4fb7-910f-4ca8ea58c25e`.

### Tags

Seven, all already in the vocabulary — no new tag needs to be created or flagged:

| Slug | ID (staging) |
|---|---|
| `licitacao` | `4b76d0d7-7a4b-4a73-a2c5-33a08853dc77` |
| `contrato` | `0831b835-2079-44f3-b5e8-3f598435bbe0` |
| `compra` | `c6416645-6aeb-43d4-a60c-8a5fddaf959a` |
| `administracao_publica` | `94b742db-a2c2-468b-b83e-2f223bb98fe7` |
| `financas_publicas` | `5dce4b1d-131b-452a-a419-bdd587a8c272` |
| `despesa` | `2195dbbf-7f5f-437c-a71e-e1aab0ac2337` |
| `transparencia` | `8b187427-519e-48cb-b0a6-5380086edf3b` |

Deliberately NOT tagged: `governo` (restates the `government` theme), `federal`
and `municipio` (geography/level, and wrong anyway — PNCP spans all three
levels), and the near-duplicates `financas` / `financa_publicas` / `gasto`.

IDs differ per backend. Re-resolve every one of these on prod before
registering there; the organization in particular must be created again.

## Measured API behaviour (2026-08-28)

Everything here was measured against the live API, not inferred. It is the
basis for the window sizes and the worker count, so re-measure before
changing either.

### tamanhoPagina is capped PER ENDPOINT, and exceeding it is a 400

From the OpenAPI spec at `https://pncp.gov.br/api/consulta/v3/api-docs`
(fetchable without credentials, unlike the gov.br portal pages, which answer
401 to anything that is not a browser):

| endpoint | max | min |
|---|---|---|
| `/v1/contratos`, `/v1/contratos/atualizacao` | 500 | 10 |
| `/v1/atas`, `/v1/atas/atualizacao` | 500 | 10 |
| `/v1/pca/atualizacao` | 500 | 10 |
| `/v1/instrumentoscobranca/inclusao` | 100 | 10 |
| `/v1/contratacoes/publicacao`, `/v1/contratacoes/atualizacao` | **50** | 10 |

There is no clamping: above the cap the endpoint answers
`400 Tamanho de página inválido` and returns nothing. `contratacao`
inherited the 500 default and would have failed every window on its first
request. A test now asserts each configured size against this table, for
both a table's pipeline path and its backfill path.

### Latency depends on pagination depth, but only where pages are large

| endpoint | page size | s/page | depth penalty |
|---|---|---|---|
| `contratos` | 500 | ~8 shallow, 14-20 deep | yes, ~2.5x by page 120+ |
| `contratacoes/publicacao` | 50 | ~3.0, flat to page 120+ | none observed |

So the two tables want opposite treatment, and guessing one from the other
is how the estimate went wrong twice. `contrato` needs windows kept shallow
(hence the `resize`); `contratacao` does not, and its 10-day windows stand.

Per record, `contrato` moves ~36 rec/s and `contratacao` ~17 rec/s -- the
smaller page cap costs roughly 2x, not the 10x the page count suggests.

### Concurrency ceiling is about 6 in-flight requests

At 9 concurrent, half the requests return 500. At 6, all succeed but median
latency roughly doubles, so total throughput barely moves -- the API is
capacity-bound. **Three workers is the right number**; raising it converts
throughput into window splits, which discard fetched pages. This supersedes
the earlier explanation that the 4-worker slowdown was only the pacer
compounding.

### instrumentoscobranca is fragile beyond the shared ceiling

The ~6 in-flight ceiling is API-wide, but this one endpoint is tighter still.
At 3 workers it failed 3 of ~65 windows with 504s and dropped connections,
while `contrato` ran 305 windows at the same concurrency with none. The same
windows serve fine when the endpoint is not being hit in parallel, and it
also carries the lowest page-size cap (100). It is therefore pinned to a
single worker via `ENDPOINTS[...]["max_workers"]`, which `harvest()` applies
as `min(caller, endpoint)` so a cap can only narrow.

### No bulk download exists

The OpenAPI spec has 12 endpoints and none serves files; PNCP's documented
open-data access is the REST API. `dados.gov.br` carries
`compras-publicas-do-governo-federal`, which is Compras.gov.br (federal
only) and therefore not a substitute for PNCP's three-level coverage.

## Cleaning verified at scale (2026-08-28)

The cleaning step is the one that previously exhausted RAM and killed the
machine, because it accumulated every row in a dict before writing. It now
streams to per-partition parquet parts. Re-verified against 185 contrato
chunks (603 MB gzipped NDJSON), mid-harvest:

```
contrato: raw=3,317,418 -> staging rows=3,317,418 (undated dropped=0) years=2021..2026
252s wall, maximum resident set size 574,603,264 (548 MB)
```

RSS held at 230-290 MB through the run and peaked at 548 MB on 3.3M rows, so
it is bounded by the per-partition buffer rather than by the table. `raw` and
`written` are equal and nothing was dropped for a missing date.

Output checked directly rather than assumed:

- 70 parquet parts across `ano=2021..2026`
- 45 columns, **every one STRING** (the architecture's 46 minus `ano`)
- `ano` absent from the file schema -- it is the hive partition only, which
  is what the earlier `ArrowTypeError: Field ano has incompatible types`
  came from
- SNAPPY, and the part row counts sum to exactly the 3,317,418 reported

Re-run after the harvest completes; this was a mid-flight snapshot, not the
final table.

## Silent data loss: empty chunks (found and fixed 2026-08-28)

A chunk file that exists is skipped on every later run -- that is what makes
the backfill resumable, and it is also what made this dangerous.

`fetch_range` used to return `[]` for a single day it could not fetch, which
is indistinguishable from "this day has no records". `run_job` wrote that as
an empty chunk, and every subsequent run skipped it. **One transient 500
became permanent data loss, reported as success.**

It had already fired. `instrumento_cobranca` held 51 empty chunks out of 56,
and the API answers several of those exact windows with a server error
rather than a no-data response:

```
20230121..20230219   HTTP 500 Erro na comunicação com o banco de dados
20240121..20240219   HTTP 404 Nenhum instrumento de Cobrança encontrado
20250121..20250219   HTTP 500 Erro na comunicação com o banco de dados
20260121..20260219   totalRegistros=17991
```

Only the 404 deserves an empty chunk. The two 500s are the server breaking,
and the old code recorded them identically.

**The fix**: an unservable single day now raises, so the window is marked
failed, no chunk is written, and the next run retries it. A day that truly
has no records never reaches that path -- PNCP signals no-data with 204, an
empty body, or a 404, all of which become `EMPTY_PAGE` and still write their
(legitimately empty) chunk, so empty ranges are not re-fetched forever.

**Remediation**: every empty chunk was deleted and re-harvested under the
fixed code -- 51 for `instrumento_cobranca`, 31 for `ata_registro_preco`, 16
for `contrato` (the latter two all in 2021, before PNCP carried data). The
list is kept at `~/Downloads/br_pncp_data/deleted_empty_chunks.json`.

**When reviewing a finished harvest, audit for empty chunks.** They are
legitimate only where the source genuinely has no data for the period;
anywhere else they are the signature of this class of bug.

## Backfill cutoff is pinned to 2026-08-28

Always pass `--end` explicitly. It defaults to today, and a chunk's filename
IS its window, so when the date rolls over mid-backfill the final partial
window is renamed and re-fetched -- wasted work, an orphaned chunk, and a
spurious failure report. Observed on `instrumento_cobranca`, whose tail
window went from `20260803_20260828` to `20260803_20260829` overnight and
then failed.

Everything after the cutoff is the recurring pipeline's job: its lookback
window covers the handover.

## Temporal coverage differs per table — do not register 2021 for all of them

Each table starts when PNCP began carrying that kind of record, not when the
portal launched. Registering a uniform 2021 start would overstate coverage on
the site. Measured from the cleaned output, not assumed:

| table | first year | staging rows | note |
|---|---|---|---|
| `contrato` | 2021 | 4,707,847 | 305/305 windows, 0 missing |
| `instrumento_cobranca` | **2025** | 215,382 | 69/69 windows; nothing in 2021-2024, where the API answers 404 "Nenhum instrumento de Cobrança encontrado" |
| `ata_registro_preco` | 2022 | 1,137,524 | 1032/1033 windows; 2021 is genuinely empty, so the table starts in 2022 |
| `contratacao` | TBD | -- | not yet harvested |

`contrato` by year, which is the shape PNCP adoption should produce and so
doubles as a sanity check:

| 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (to Aug) |
|---|---|---|---|---|---|
| 5,308 | 40,645 | 242,957 | 1,010,769 | 2,023,341 | 1,384,827 |

`ata_registro_preco` by year, the same shape:

| 2022 | 2023 | 2024 | 2025 | 2026 (to Aug) |
|---|---|---|---|---|
| 205 | 37,194 | 302,487 | 483,117 | 314,521 |

Lei 14.133 became mandatory in April 2023, which is where the curve turns.
2026 annualises to ~2.1M, consistent with 2025. The 2021 and 2022 counts
reproduce the earlier dev-table validation exactly, so the re-harvest after
the empty-chunk fix did not perturb what was already correct.

Read the real min and max from the cleaned parquet before registering
`create_update_datetime_range`, per table. Fill this table in as each one
completes.

## A backfill is not done until a re-run reports zero failures

Windows fail transiently -- PNCP returns 504s and dropped connections under
load, and a window that fails writes no chunk. That is by design (see the
empty-chunk section), and it means **the last run of a backfill must be one
that harvests nothing**:

```bash
PNCP_DATA_DIR=~/Downloads/br_pncp_data PNCP_WORKERS=3 \
  uv run python models/br_pncp/code/download.py --end 2026-08-28
```

Re-run until every table reports `0 new records this run` and no `FAILED`
lines. Everything already on disk is skipped, so a sweep over a complete
backfill costs one page-1 request per window and nothing else.

Evidence this is not paranoia: all three `instrumento_cobranca` windows that
failed at 3 workers succeeded on the retry at 1, and `ata_registro_preco`
window `20210105_20210106` failed on a day that is almost certainly empty.
Transient means transient in both directions.

Then verify coverage per table rather than trusting the counts:

```python
# expected window tags vs what is on disk -- must be 0 missing, 0 orphaned
utils.windows(start, end, spec["window_days"], resize)
```

## Known limitation: dedup is within a partition

The models dedupe with `qualify row_number() over (partition by <pncp id>
order by data_atualizacao desc) = 1`, and `insert_overwrite` replaces whole
`ano` partitions. Both are keyed on `ano`, which is derived from the
publication date.

That is correct as long as a record's publication date never changes. If
PNCP ever corrects one across a year boundary, the record lands in a new
partition while the old copy survives in the previous one, and the
`unique_combination_of_columns: [ano, <pncp id>]` test will not catch it --
each copy is unique within its own year.

Not engineered around, because the alternative (deduping across the whole
table on every run) costs a full scan of a multi-million-row table on every
refresh to fix an event that may never occur. Recorded so that a future
duplicate-control-number report has an explanation rather than looking like
a dedup bug.

## Dev staging loaded (2026-08-31)

Three of the four fact tables are complete and in `basedosdados-dev`, each
row count asserted against what the cleaning step reported rather than eyeballed:

| staging table | rows |
|---|---|
| `br_pncp_staging.contrato` | 4,707,847 |
| `br_pncp_staging.ata_registro_preco` | 1,137,524 |
| `br_pncp_staging.instrumento_cobranca` | 215,382 |

`contratacao` and `dicionario` follow when the harvest lands. Uploading early
was deliberate: the upload path had never run at this scale, and a
credentials or convention problem is much cheaper to find now than after
another 30 hours of harvesting.

Schema verified on the external table rather than assumed: 23 columns, every
one STRING, and **`ano` surfaces as a column** even though it is excluded
from the parquet files. That round trip -- hive partition in the directory
name, column on the external table -- is what `safe_cast(ano as int64)` in
the dbt model depends on.

No `00_header.parquet` is needed (see the part-size section): the largest
part is 50,000 rows / 6 MB.

## Running dbt and the upload locally

Two different credential mechanisms, and neither is picked up by default:

```bash
# upload.py -> basedosdados / google-cloud-storage
export GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json

# dbt -> profiles.yml reads BD_SERVICE_ACCOUNT_DEV, whose default is the CI
# path /credentials-dev/dev.json, which does not exist on a laptop
export BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/staging.json
```

Without the second, dbt fails with
`Database Error [Errno 2] No such file or directory: '/credentials-dev/dev.json'`
before running anything, which reads like a dbt problem rather than a
missing environment variable.

Note the repo's `profiles.yml` uses `BD_SERVICE_ACCOUNT_DEV` for the **prod**
target too; that looks like a copy-paste slip upstream. It does not matter
here, since prod is materialised by table-approve on merge and never from a
laptop.

## Materialized in dev, and the dedup checked (2026-08-31)

`dbt run` on the three finished models: PASS=3, ERROR=0 (contrato 5.2 GiB
processed in 47s).

| model | staging | materialized | collapsed |
|---|---|---|---|
| `contrato` | 4,707,847 | 4,707,847 | 0% |
| `ata_registro_preco` | 1,137,524 | 1,137,524 | 0% |
| `instrumento_cobranca` | 215,382 | 179,463 | 16.7% |

**0% is the expected answer for the first two, not a sign the dedup is
inert.** Each is harvested by a date that pins a record to exactly one
window -- publication date for `contrato`, `dataAtualizacaoGlobal` for
`ata` -- so no record is delivered twice and the `QUALIFY` has nothing to
collapse. It also means `id_contrato_pncp` and `id_ata_pncp` are already
unique within a partition.

**The 16.7% on `instrumento_cobranca` was verified rather than assumed**,
because a composite key (cnpj_orgao, ano_contrato, sequencial_contrato,
sequencial_instrumento_cobranca) could silently merge distinct records:

```
keys with duplicates                      33,068
rows collapsed by dedup                   35,919
  keys whose rows genuinely DIFFER             0
```

Every collapsed row is byte-identical to the one kept, so the key is
merging true duplicates and nothing is lost. Re-run that check if the key
ever changes.

## Sustained rate limiting, and backing off (2026-09-02)

After five days of continuous harvesting, PNCP is limiting us hard. Per-page
time went from ~3s at the start to **~55s**, with the shared pacer backed off
to 0.66-0.89s and 16 cooldowns (60/180/420s sleeps) recorded. Throughput
settled at 30-35 chunks/hr, which put the remaining `contratacao` work at
about six days.

**The pacer is a shared, aggregate limiter, not per worker.** `THROTTLE`
gates every request in the process, so `PNCP_MIN_INTERVAL` sets the total
request rate and `PNCP_WORKERS` only sets how many can be in flight at once.
That is why the response to being limited is to raise the interval, not to
cut workers.

Running with `PNCP_WORKERS=2 PNCP_MIN_INTERVAL=1.0` — an aggregate ceiling of
~1 request/second. The bet is that a sustained penalty is costing far more
than the nominal rate reduction: at ~55s/page across 3 workers we were
achieving roughly 0.05 req/s, so even a strict 1 req/s ceiling would be an
order of magnitude better. Measure before believing it.

Related: 120 windows are queued as failed and need a sweep run once the main
pass completes. They write no chunk, so re-running the same command picks
them up.
