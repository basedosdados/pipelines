# Source Lessons — `br_bd_execucao_estadual`

Defects and access constraints found in the state sources, written down so a future run
does not rediscover them. Every entry here was **measured**, not inferred; where a number
appears, it came from the data.

Read this before touching `code/download_*.py` or `code/clean_*.py`. The per-state
`constants.py` sections carry the same facts inline, next to the values they explain;
this file is the cross-cutting view and the place for lessons that span states.

`ONBOARDING_PLAN.md` describes the dataset's design. This file describes what fights back.

---

## 0. The rules that generalise

These cost the most time and recur across states.

1. **A "no" from a strict reader is information; a "yes" from a permissive one is not.**
   Python's `latin-1` decodes any byte sequence, so "try utf-8, else latin-1" can never
   fail and will happily ship mojibake. duckdb's latin-1 is strict and *refuses*
   (`File is not latin-1 encoded`) — that refusal is how both RS and SC were correctly
   identified as cp1252. "utf-8 strict, else cp1252" IS a valid probe, because UTF-8 is
   self-validating and cp1252 leaves five bytes undefined.

2. **`strict_mode=false` and `ignore_errors=true` mis-parse rather than reject.** They
   convert a loud failure into a silent wrong answer. When a CSV will not parse, fix the
   file (re-emit it), never the parser's standards. See BA and SC below.

3. **Verify against a control total the source itself publishes.** Row counts, money
   totals, per-category subtotals. A source that gives you a number to check against
   (SC's `lista.total`, SIGEO's grid totals) turns "looks fine" into "reconciles".

4. **A number that looks plausible is the dangerous kind.** SC's comma decimals silently
   NULLed every non-integer value and turned R$2.90bn into R$632M — a figure entirely
   consistent with a mid-sized state's monthly spend. Only the published subtotal caught it.

5. **Check key uniqueness before every join, and measure the fan-out.** Document numbers
   in these sources are routinely unique only *within* a unidade gestora. MG's glob
   collision and SC's per-UG numbering are the same class of bug.

6. **Pace every scraped or rate-limited source; never burst.** Two separate IP-wide
   blocks were self-inflicted on CE by concurrency that a serial loop would have avoided.

7. **Number format differs per state and must be checked, never reused.** MG plain
   (`52.50`) · PE US with leading space (` 43200.0`) · BA, ES, RS, SC comma decimal with
   no thousands separator (`2643000,00`) · **SP `.` thousands AND `,` decimal**
   (`2.693.456,58`), where 76% of values carry a separator, so BA's
   `replace(',', '.')` would null three quarters of the table.

---

## 1. Access and reachability

### "Unreachable" is three different verdicts

Split by `getaddrinfo` → TCP → TLS before concluding anything. DNS NXDOMAIN means the
door is elsewhere (RJ's catalogue is at `dadosabertos.rj.gov.br`; `dados.rj.gov.br` does
not exist). A TCP timeout on a host that resolves is geo/WAF. Only the first is a reason
to stop looking.

### Gate a probe on a canary host, not on egress country

Through a working Brazilian tunnel, every IP-echo service may still report the local
country: the tunnel can be split by destination. An `egress == BR` guard aborts a
perfectly good session. Test reachability of a host known to refuse you instead.

**Use CE as that canary, not RS.** RS turned out to be *path*-dependent, never
country-dependent — it refused a residential Australian ISP, worked from a university
range, and answers the GKE worker today. CE is the only confirmed country/ASN block in
the set: 403 from Australia, 403 through an Australian split tunnel, 200 from São Paulo.

### The Brazilian-IP requirement is a PIPELINE constraint

A laptop VPN solves the one-off historical load only. The recurring flow fetches from the
GKE cluster, so a geo-fenced state has no refresh unless the *cluster's* egress is
Brazilian. There is no proxy or geo handling anywhere in `pipelines/` today. Until that is
settled, a geo-fenced state ships **historical-load-only**, and the dataset description
says so rather than carrying a schedule that will silently never refresh.

| Needs a Brazilian IP | Does not |
|---|---|
| CE, RO, RJ | MG, BA, PE, SP, ES, RS, SC, PB |

---

## 2. Per-state defects

### MG
- **Glob collision.** `dm_empenho_desp_compras_empenho.csv.gz` sits next to
  `dm_empenho_desp_<ano>.csv.gz` with an identical schema. A `dm_empenho_desp_*` glob
  swallows it and adds 1,098,339 duplicate `id_empenho`, which fans the 80M-row fact
  table out through a LEFT JOIN and inflates every total. Glob
  `{stem}_[0-9][0-9][0-9][0-9].csv.gz`. The only visible symptom was "26 files" where 25
  exercises were expected.
- **Truncated downloads pass a "file exists and is non-empty" check.** Walk the whole
  gzip member (`download_mg.py::is_intact`).
- A browser User-Agent is required; a bare curl UA gets 403.

### BA
- Values and creditors are in **different views that cannot be joined**:
  `VW_PAINEL_DESPESA` has the money with no creditor and no empenho;
  `VW_PROCESSO_SEI` has the empenho and CNPJ with no values, and only from 2019.
  1,091,372 empenhos map to 185,969 dotação keys. BA therefore feeds `despesa_mensal`
  and `empenho_credor`, never `despesa`.
- **`vencedor` must be READ, never derived.** 84% of `Perdedor` rows carry a positive
  `val_total_homologado`, because that column is the *item's* homologated amount. The
  inferred flag gave 1.67 winners per item; the published label gives 0.96.
- Item descriptions carry embedded newlines and unescaped inner quotes. Rewrite splitting
  on the unambiguous `";"` separator, then check the numeric columns are still numeric to
  prove no field shift.

### PE
- **Three schema eras with disjoint column names** (2008 / 2009-2010 / 2011+). Modelling
  only the modern names leaves **1,031,326 rows — 21% of PE — present and entirely NULL**.
  Caught by per-year coverage of key columns, never by the row total.
- Two exercises are named year-first (`2009-base-despesa.csv`); match a 4-digit year
  *anywhere* and assert the expected span.
- Encoding, line ending and separator all vary by exercise with no pattern.
- **`SELECT {year} AS ano` types the literal INTEGER in duckdb** while `all_varchar`
  makes everything else VARCHAR, so the staging parquet was not all-STRING and prod died
  on the first dbt model. Quote the literal. Four green dev runs proved nothing, because
  dev's external tables were built by a schema-inferring path.

### SP
- SIGEO is an aggregate, not a ledger: no empenho document, no sub-annual date,
  and `ddlLicitacao` is the *modality*, not a tender id. It feeds `despesa_anual` only.
- The postback chain is load-bearing and **order matters**. Tick execution phases only:
  including Dotação Inicial/Atual silently drops the Licitação and Item columns while
  still exporting successfully. Validate on `ddlLicitacao`, not on `CGC`.
- An empty órgão is **not** a fetch failure — a secretariat only answers for exercises in
  which it existed. 509 with data + 35 empty = the whole grid.

### ES
- `Despesas-2013.csv` appears **twice** as two resources of different sizes, and 2024 is
  published three times (annual plus two stale semester files). A name-keyed dict silently
  keeps one; a `Despesas-*` glob double-counts the other.
- 2004-2008 are **not** transaction grain despite identical columns —
  `Favorecido = "Informação não disponivel."`. Real transactions start 2009.
- **`01/01/1753` is SQL Server's datetime minimum and ES buckets by it.**
  `Contratos-1753.csv` holds 180 real contracts. A plausibility filter on the file-name
  year deletes them in silence. In staging the stamp is called `ano_arquivo`, never `ano`.

### RS
- **cp1252, not latin-1** (see rule 0.1).
- **Schema drifts three times** (62 → 66 → 67 columns). Every partition must be written
  to the superset or the BigQuery wildcard load silently keeps one schema and NULLs the rest.
- **Ragged rows**: unescaped `;` in the trailing free-text column. Rejoining with `;`
  puts the separator back *into* the field and is a no-op — the fix is to re-emit with a
  delimiter absent from the data.
- **Control bytes as data** (0x01, 0x1E, 0x1F in 2016-02), so the output delimiter is
  chosen per file and *verified* against the whole text, not sampled.
- **Six months are missing from RS's own catalogue** (2020-06, 2020-08, 2022-04, 2023-02,
  2023-06, 2023-08); those slots serve the neighbouring month's file, and the two copies
  differ. 175 archives, every CRC valid, zero download failures — only grouping the DATA
  by its own `(Exercicio, Mes)` found it.
- **Retenção is EXCLUDED** from `despesa`: it is withheld from inside a payment RS also
  reports in full. Contrast SC, below — the decision is source-specific, not a house rule.

### SC
- **The CKAN bulk files cannot be parsed and are not used.** `dehistoricoempenho` is free
  text carrying both embedded newlines and semicolons while the fields are effectively
  unquoted (705 double quotes in 400k lines); splitting on `;` yields the correct 34
  fields on only 32,444 of ~200,000 physical lines. Ingest from the portal's export
  endpoint instead, which is quoted, covers 2011+ instead of 2021+, and carries all three
  phases instead of empenho alone.
- **Two quote-escaping conventions, mixed, sometimes in one file**: doubled (`""`) and
  backslashed (`\"`). 2011-01 has 7 of the second and 5 of the first; 2013-06 has 55
  doubled and none backslashed; 2016-01 has neither. No single duckdb setting reads both.
  Python's `csv` does (`doublequote=True` + `escapechar='\\'`), so the parse happens in
  Python and the rows are re-emitted quoting-free for duckdb.
- **cp1252 despite `Content-Type: text/csv; charset=UTF-8`.** The header is wrong.
- **The empenho number is not unique.** 2011 has 210,108 rows over 21,753 distinct
  `nunotaempenho`, and 19,533 of those appear under 2+ unidades gestoras — numbering
  restarts per UG. Join on the composite `ugempenho` (`450022|2011NE000085`). Joining on
  the bare number multiplies rows roughly tenfold.
- **One row per movement, not per empenho.** `cdtipoempenho` is Emissão / Reforço /
  Anulação / Estorno, each its own document, linked to the original by
  `nunotaempenhooriginal`. Anulação and estorno are **already signed negative** — the net
  is a plain SUM. In 2011, 47,527 modifications resolve 100% to an emissão and the
  grouping yields exactly the 162,581 emissões.
- **Retenção is INCLUDED**, the opposite of RS, and for a measured reason: in SC every
  `nupagamento` is Líquido OR Retenção OR Estorno and never a mix, so Líquido is the net
  paid to the creditor and Retenção is the complement. Liquidação nets R$3.06bn for
  2024-03 against Líquido's R$2.42bn and a gross of R$2.91bn.
- `situacao` in the `pagamento` table means different things in PE and SC, so a
  `situacao = 'PAGA'` filter drops SC entirely.

### CE
- **Double-gated**: a country block *and* a JS anti-bot challenge on the HTML. The
  challenge serves an identical ~247 KB page for every URL with HTTP 200 — the tell is
  that the length does not change with the dataset id. `curl_cffi` TLS impersonation does
  **not** pass it.
- **`/attachments/` is challenge-free but still geo-fenced** — the same URL returns 200
  unauthenticated from São Paulo and **403** from a US IP. So CE cannot refresh from a
  non-Brazilian cluster even for downloads.
- **Enumerate with same-origin `fetch()` from inside the browser**, ~300 ms apart; 200
  requests produced zero challenges. Exported cookies work in `requests` but burn after a
  handful of calls. Chunk to ~40 ids per call (45 s tool timeout).
- **A blocked `/attachments/` response is a success-looking stream whose first four bytes
  are `666f7262`** — ASCII `forb`. A downloader checking only `status_code` writes files
  that look like data. Check magic bytes and refuse bodies under ~512 B.
- The catalogue list page is AJAX-rendered (so `requests` sees an empty table even with a
  valid cookie) while detail pages are server-rendered. Enumerate by id: 1-200 with 52
  gaps yields exactly the 148 datasets the portal reports.
- `api-dados-abertos.cearatransparente.ce.gov.br` is a **pilot covering only contratos,
  convênios and servidores** — no empenho, liquidação or pagamento. Do not mistake it for
  the data route.
- Five file formats across the series (`.rar` 2006-2017, `.csv`, `.csv.zip`, `.xlsx`
  2019-2023, `.ods` for 2023-Q4, `.xls` 2024+), chunked by trimestre for empenho and
  bimestre for NPD/NLD, sometimes split into parts.
- **Dataset 135 ("Notas de Liquidação 2020") contains two files named `NLD_Ano2019_*`** —
  the RS missing-month trap in a new costume. Group by the data's own year, not the slot's.
- Attachment URLs are content-addressed (`/attachments/<sha1>/store/<sha256>/<name>`) and
  rotate on republish, which is daily for the empenho dataset. A saved URL manifest goes
  stale.

---

## 3. Infrastructure traps that bit this dataset

- **`table-approve` cannot promote this dataset.** It syncs `staging/<dataset>/<PUBLISHED
  table>/`, and this dataset has ~49 staging mirrors named after SOURCE tables feeding 10
  published models through ephemeral per-state models. The sync matches nothing. **The
  Prefect flow is the only route to prod**, and the first prod run must be
  `full_refresh=True`.
- **BigQuery wildcard parquet loads infer ONE schema and drop the rest silently**, while
  reporting the full row count. `upload.py` now compares the loaded BQ schema against the
  union of the uploaded parquet schemas and refuses the load if anything was dropped.
- **Staging must be all-STRING**, including the one-shot onboarding upload, or a typed
  external table collides with the pipeline's later all-STRING overwrite.
- **An empty first parquet partition poisons the staging schema** (`dump_header` infers
  INTEGER for every column). Cleaners drop 0-row partitions.
- **`memory` is not a work-pool job variable** — set `memory_limit` and `memory_request`
  or the pod silently gets 4Gi whatever number you write.
- **The union in `despesa.sql` resolves POSITIONALLY**, so every state model must project
  the 33 columns in the exact order of the first term. ES shipped with 14 of 25 columns
  shifted from position 11 because it followed `schema.yml` instead of the first term.

---

## 4. Sources surveyed and rejected

Re-probed from a Brazilian IP on 2026-09-15. Do not re-survey these without a reason.

| UF | Verdict |
|---|---|
| **RO** | Usable. `transparencia.api.ro.gov.br/api/v1`, published OpenAPI, 9 endpoints. `pagamento-fornecedor` is transaction grain with credor, empenho, OB and liquidação on one row; `despesas` is a day × budget-line aggregate. 2019/2020+. `PageSize` capped at 100. Needs a BR IP. |
| **RJ** | Thin. `dadosabertos.rj.gov.br` has 1,119 packages but the fiscal content is a per-agency PDF dump. The one real series is SEFAZ's `tfe-despesa` (`despesa_generica<ano>.csv`, 2016-2025, ~10 MB/yr) — a **month × budget-line aggregate**, `despesa_mensal` grain, no creditor, no empenho. Five preamble lines before the header; 2019 and 2021 each published twice; TLS drops mid-download. |
| **PB** | Usable, cheapest in the set, no BR IP. REST API at `api.dados.pb.gov.br/api/v1`, `ano`+`mes` required, ~2M rows, 2015+. Complete empenho→liquidação→pagamento chain plus `codigoMunicipio`. Tenders are winner-only. |
| DF, GO, PR, SE, RN, AM, MS, MA, PA, PI, TO | Portals answer; **no bulk fiscal files**. DF is Liferay; GO an Angular SPA on a WSO2 gateway; PR's `www.dados.pr.gov.br` is a Drupal brochure with no dataset content type (the real data is still the `jsessionid` JSF portal); PI, TO and GO serve the same page for every path. SP-SIGEO-class effort each. |
| MT, AC | Dead ends. MT has 26 packages and nothing fiscal; AC has 20 and one per-capita health indicator. |
| AL, AP, RR | No candidate host completes a TLS handshake, from Brazil either. Broken, not geo-blocked. |
