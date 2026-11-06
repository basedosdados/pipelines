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

6. **Pace every scraped or rate-limited source; never burst -- but confirm a refusal is
   a block before treating it as one.** Two CE incidents were recorded as self-inflicted
   IP-wide blocks. They were not: the downloader was omitting a required query parameter,
   and the resulting HTTP 403 is byte-identical to a block. The whole 216-file set then
   downloaded serially with zero refusals. Pacing is still right -- a serial 1.2 s loop
   costs nothing here -- but "we got blocked" is a diagnosis, and it needs the same
   evidence as any other. See CE below.

7. **One file, one parser definition.** If a downloader counts rows and a cleaner
   parses them, they must use the *same* reader settings. A default `csv.reader` and one
   with `escapechar='\\'` disagree on exactly the pathological rows — one row in ~82,000
   on SC — and the mismatch guard then rejects a file that is perfectly fine, four times
   in a row, with a message blaming the source.

8. **A source can use the same character as an escape AND as data — choose the
   convention per FILE, and verify by parsing the whole file.** SC's backslash escapes a
   quote in `empenho_201101` (`\\"Split\\"`) and is literal in `liquidacao_201106`
   (`"3932532\\"`, a document number). Each global setting loses records in the file the
   other handles.

   **Per-record repair is not enough, and knowing why matters.** Under the wrong
   convention a record does not merely land with the wrong field count — it can split
   into TWO records, so the boundary itself is wrong and there is no raw record left to
   re-read. `liquidacao_202402` row 10018 does exactly that (82,095 records instead of
   82,094) because its free text holds both `\\"` and a real newline. Parse the file
   under each candidate, count records that miss the expected width, and keep the
   convention that places all of them.

9. **Number format differs per state and must be checked, never reused.** MG plain
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

### PB
- **The survey's row estimate was 5x low.** It extrapolated from 2024-01 (5,514 empenhos),
  which is atypically small; the year is **320,201**. January is not a representative
  month in a Brazilian exercise — never size a source from one.
- **`codigoLicitacao` is a MODALITY, not a tender id** — 17 distinct values whose labels
  are DISPENSA - SERVICOS, PREGAO - PRESENCIAL, OBRAS - CONCORRENCIA. The SP
  `ddlLicitacao` trap. The real tender link is `numeroProcessoCompras`, which is
  space-padded and present on **446 of 320,201** rows (0.14%).
- **`contrato` is junk on every row**: `'SN'`, `'S/N'`, `'sn'`, `'NT'`, `'-'`. It is
  populated 100% of the time, so a coverage check passes while the field carries nothing.
- **The payment ledger uses a different organisational code system from the empenho.**
  `ordem_cronologica_pagamentos.codigoUnidadeGestora` has **0** overlap with the
  empenho's `codigoUnidade` and 6 of 17 with `codigoOrgao`, while
  `liquidacoes.codigoOrgao` matches **42 of 42**. So liquidação joins and pagamento does
  not, and `pagamento_pb.id_empenho_bd` is null rather than wrong. The survey's
  "complete empenho -> liquidação -> pagamento chain" is only half true.
- **`numeroEmpenho` fans out ~8x on its own**; the key is
  (ano, codigoOrgao, codigoUnidade, numeroEmpenho), and dropping the órgão still
  collides ~5,700 rows a year.
- **`valorDespesa` is the net** (`valorEmpenhado + valorAnulado`, exact to the cent).
  `valorEmpenhado` is gross.
- **`participantes` and `documentos` are JSON STRINGS, not arrays.** Iterating the raw
  value walks characters; a length check reports 240 (the string) instead of 1 (the
  record). Participants carry no win/lose flag and run ~1.8 per tender, so they are
  awarded suppliers, not a bidder list — `vencedor` stays null (the BA rule).
- 2014 and earlier return **HTTP 400**, not an empty result, so a reader that treats
  non-200 as "no data" records those years as genuine gaps.
- `per_page` caps at 1000; 2000 returns 400 rather than silently truncating.

### RJ
- **The catalogue is at `dadosabertos.rj.gov.br`.** `dados.rj.gov.br` is NXDOMAIN and
  `transparencia.rj.gov.br` is a WordPress brochure, which is why earlier surveys filed
  RJ as geo-blocked. It was the hostname, not a block — though the host does separately
  require a Brazilian IP.
- **`tfe-despesa` is a cumulative year-end SNAPSHOT, not a monthly series.** Every file
  carries exactly one `Posição`: `12/YYYY` for a closed exercise, the latest month for
  the open one (`07/2025`). So the grain is budget line x exercise and the values are
  positions, not movements. Reading `Posição` as "the month this row belongs to" and
  unioning it into a monthly table would put a cumulative annual figure beside monthly
  movements under the same column names.
- **It therefore fits none of the existing tables.** Not `despesa` (no empenho, no
  creditor, no sub-annual date); not `despesa_mensal` (that is month x line, RJ has one
  month per year); not `despesa_anual` (that is creditor x line x year, RJ has no
  creditor). Where it belongs is a schema decision and is deliberately left open —
  `clean_rj.py` stages it and stops.
- **2019 and 2021 are each published twice** as separate resources of identical size —
  the ES `Despesas-2013.csv` trap. De-duplicate on the CKAN resource id, not the name.
- **Five preamble lines precede the header** (ministry, secretariat, subsecretariat,
  "Transparência Fiscal", date range). A reader that assumes row 0 is the header treats
  the first data row as column names.
- TLS connections drop mid-download; every fetch needs retries and a short file must be
  rejected rather than kept. Unlike RS and SC the files are plain latin-1 — duckdb's
  strict reader accepts all ten — and every row has exactly 41 fields.
- Size: 176,525 rows over 2016-2025, ~101 MB. Totals run R$60.8bn (2016) to R$107.3bn
  (2024) empenhado, consistent with the state budget.

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

#### CE, measured on the full 216-file download (2026-09-16)

**Access**

- **`?force_download=true` is REQUIRED on every `/attachments/` URL.** Without it the
  server answers **HTTP 403 with a 9-byte `forbidden` body** for a URL that serves the
  file perfectly with it. Verified by alternating the two forms against one URL four
  times in a row: `403 / 206 / 403 / 206`, seconds apart. It is the parameter, not a
  rate limit and not an IP block.

  **This overturns the previous entry.** CE was recorded as having IP-blocked us twice
  in one day; the downloader was building URLs without the parameter, so every request
  403'd and the escalating 60/120/180/240 s backoff made it look like a hardening block.
  The 9-byte `forbidden` body is real and the magic-byte check is still right -- but
  check the query parameter before concluding CE has blocked you. All 216 manifest URLs
  captured 12 hours earlier were still live and downloaded serially at ~1.2 s with zero
  refusals.

- **The challenge tell is the invariant LENGTH, not the status.** Dataset detail pages
  return HTTP 200 at 246,688 bytes (id 168) and 246,689 (id 179) -- two completely
  different datasets, a one-byte difference. The portal **homepage is not challenged**
  (41,195 bytes of real HTML), so "the host answers" proves nothing about the catalogue.

**Shape of the series**

- **Eleven schemas across three phases**: six for empenho, three for pagamento, two for
  liquidação. They are not a restyling of one schema -- **the eras publish different
  things**. 2006-2013 empenho carries `cod_gestora`/`cod_credor`/`cod_item_natureza`
  plus `cod_ne_original` and `cod_tipo_empenho`, i.e. an SC-style movement model.
  2014-2018 carries codes *and* names plus tender and contract columns. 2019+ carries
  **labels only** -- no CNPJ, no budget code, no contract, no tender. A dbt model that
  treats CE as one series will silently lose whichever half it did not model.

- **2019 Q4 empenho is a different product entirely.** `4º Trimestre 2019.xlsx` has
  **7 columns** (`exercicio, numero, data_de_emissao, secretaria_orgao,
  unidade_executora, beneficiario, valor_pago`) and 23,810 rows, against 26,482 /
  91,183 / 66,718 for Q1-Q3 of the same year in the 22-column schema. It has no
  `valor_empenhado` at all. So **2019 empenho is effectively three quarters**, and any
  annual total for 2019 is understated by roughly a quarter with nothing null to show
  for it.

- **Dataset 31 splits one exercise across two schemas.** "Notas de Empenho - 2018" holds
  three `.rar` files covering Jan-Jul in the 32-column legacy schema *and*
  `notas-de-empenho-ago-dez-2018.csv.zip` covering Aug-Dec in the 22-column modern one.
  Complementary, not duplicated -- but a reader that picks one format per dataset loses
  half the year.

**Defects that survive a width check**

- **The legacy exports pad a varying number of unnamed, always-empty trailing columns**
  onto the header and every row: 0, 1, 2, 10, 25 and 56 measured across files of the
  same series. Taken at face value that reports six schemas for empenho where there is
  one.

- **Free text is quoted inconsistently**, so a value containing the separator splits the
  record: `pagamento de diária e ajuda de custo mês de abril, portaria 585/2018`,
  `F, TARCISIO G. PARENTE - ME`, `PAGAMENTO OBRA DE ENGENHARIA 2015, SEM RETENÇÃO DE
  ISS ...`. It is **not one column per schema**: legacy empenho splits
  `especificacaogeral` in most cases and `razaosocialcredor` in others
  (`...,3301349000151,F,8825,2200010012015C,...`). And `especificacaogeral` sometimes
  holds a comma-separated LIST -- course participants, seized equipment with serial
  numbers -- so one value can split a row into dozens of fields.

- **The worst case is a row with the RIGHT width and the WRONG columns.** A stray comma
  inside a run of empty columns shifts every later value one column right, and the
  file's trailing padding absorbs the overflow, so the row arrives at exactly the
  expected field count. Measured on `npd-2017-terceiro-trimestre` by the position of
  `DESEMBOLSO`, which the schema fixes at column 7:

  | width | col of DESEMBOLSO | rows | |
  |---|---|---|---|
  | 36 | 7 | 133,265 | correct |
  | 36 | 8 | **342** | **shifted, invisible to a width check** |
  | 37 | 9 | 367 | shifted, caught by width |
  | 38-42 | 10-14 | 165 | shifted, caught by width |

  Those 342 rows load with a CNPJ in `valor`, a date in `grupofin` and a creditor name
  in `cpfcnpjcredor` -- every column populated, nothing null, nothing to notice. **They
  are only detectable because some columns are constrained to dates, times, years and
  money.** `clean_ce._place` repairs on that basis and refuses when more than one repair
  survives the type check. 1,295 rows were repaired in total across the series.

  The generalisable rule: **where a source pads on the right, field count proves
  nothing. Validate the column types.**

**Values**

- **36,760,936 fields hold the literal string `NULL`** -- across 104 files, every
  modern (2018+) liquidação and pagamento file and no empenho file. It is not confined
  to a few columns: `efeito` carries 2,214,732 of them against ~4.5M pagamento rows,
  roughly half the column, as do `servico_bancario`, `banco_pagamento` and `data_atual`.
  Staged verbatim it is a string that is not null, so `count(x)` counts 2.2M phantom
  values and a `group by` grows a "NULL" category. `clean_ce` writes it as a real null.

- **`classiforcamcompl` is 100% destroyed by Excel.** Every one of the 37,968 rows of
  `ned-2017-primeirotrimestre` renders it in scientific notation
  (`"4,6200003041225E+040"`, with a Brazilian comma in the mantissa), collapsing the
  budget classification to **502 distinct values where `classiforcamreduz` has 4,222**.
  The full code is not recoverable; use the reduced code.

- **The decimal separator differs by era**, so there is no dataset-wide convention:
  legacy `.rar` and the 2018 CSVs use a dot (`8100.00`, `391653.84`), the modern CSVs a
  comma inside quotes (`"14145,17"`), and the `.xlsx`/`.xls` files carry native numbers.
  A dbt model applying one convention to the whole table nulls three of the four.

- One file of the 216 has its own dialect: **`notas_de_empenho_2026.csv` is
  semicolon-separated and cp1252** where the other 215 are comma and UTF-8. Both are
  probed per file.

- Grain is **empenho x budget line**: 2025 has 196,255 rows over 30,600 distinct
  `Número`. `Credor` in the modern era is a name carrying a partial CNPJ/CPF prefix in
  the same field (`54.212.382 FELLIPE BARBOSA DA SILVA`), not two columns.

**Files that share a name, and files that duplicate each other**

- **`NPD+5BI1.csv` (dataset 170) contains a damaged second copy of `NPD+5BI2.csv`.**
  It has 140,891 data rows: 39,998 genuine ones carrying `exercicio = 2025`, and
  **100,893 with the first six columns -- `exercicio`, `unidade_gestora`,
  `unidade_executora`, `numero`, `natureza`, `justificativa` -- blanked out**. Those
  100,893 reduce to 98,322 distinct values on columns 6-27, `NPD+5BI2.csv` has exactly
  100,893 rows reducing to the same 98,322, and every one is present in both. Part 2
  carries all six identifying columns populated on all 100,893 rows.

  Kept, they inflate the 5th bimestre of 2025 by 2.5x with rows attributable to no
  exercise, no unidade and no document number. `clean_ce` drops rows with no exercise
  and says how many. **This is the only place in the 216 files where the exercise is
  ever blank**, which is what makes the rule safe to apply generally.

- **`NPD+4BI.csv` is listed twice in dataset 170 -- and the two are NOT duplicates.**
  They share **zero** rows and zero `(exercicio, unidade_gestora, numero)` keys:
  60,907 rows (26.1 MB) and 63,803 rows (27.6 MB), disjoint. They are two *halves* of
  the bimestre published under one file name. Both must be staged.

  This is why the downloader names its destination `<dataset>__<sha1 prefix>__<name>`.
  A name-keyed download overwrites the first with the second and loses 60,907 rows
  with nothing to show for it -- and the loss looks exactly like the source having
  published one file.

  The general rule: **when a catalogue lists the same file name twice, measure the
  overlap before deciding it is a duplicate.** The ES `Despesas-2013.csv` case was a
  real duplicate; this one is a split, and the two need opposite handling.

- `numero_processo_administrativo_despesa` is destroyed by scientific notation in the
  modern CSVs too (`4,60420029762026E+016`), the same defect as `classiforcamcompl`
  but in a plain CSV rather than an Excel export.

**Broken headers**

- `notas-de-empenho-ago-dez-2018.csv.zip` names its creditor column
  `translation missing: pt-BR.integration/expenses/ned.spreadsheet.worksheets.default
  .header.razao_social_credor` -- a failed i18n lookup rendered as a column name.
- `npd-2013-quarto-trimestre.rar` names its first column **`are`**; the other three
  quarters of 2013 name it `num_ano`, the remaining 48 names are identical, and every
  value in the column is `2013`.

Both are renamed through `constants.CE_HEADER_FIXES` rather than carried into staging.

**Tooling**

- **`rarfile` lists a member it cannot read.** Without an external `unrar`/`unar` it
  fails mid-stream (`BadRarFile: Failed the read enough data: req=1500 got=41`) rather
  than refusing up front -- a truncated read that looks like a short file. `bsdtar`
  (libarchive, present on macOS) reads all 86 archives whole.
- A container's extension is a hint: dispatch on magic bytes, not the name.

**No control total**

- **CE publishes no row count, no money total and no per-category subtotal** -- its own
  `Inventário de dados` lists only name, content, órgão and creation date. Unlike SC
  (`lista.total`) or SP (grid totals), there is nothing external to reconcile against.
  The checks that do exist are internal to the catalogue: 2023 empenho is published
  twice (dataset 145 quarterly, dataset 152 consolidated) and dataset 170 lists
  `NPD+4BI.csv` twice with different content hashes. `validate_ce.py` uses those.

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
| **RO** | Usable but **expensive**, and the cost was underestimated. `transparencia.api.ro.gov.br/api/v1`, published OpenAPI, 9 endpoints. `pagamento-fornecedor` is transaction grain with credor, empenho, OB and liquidação on one row; `despesas` is a day × budget-line aggregate. 2019/2020+. **`PageSize` is capped at 100 and `despesas` alone is 1,725,698 rows for 2024** — ~17,000 requests per exercise and ~120,000 for the series, i.e. many hours of sequential paging. The harvest is request-bound, not byte-bound; budget for it before starting. `pagamento-fornecedor` returns an empty envelope for the date parameters that work on `despesas`, so its own parameters still need reading off the OpenAPI spec. Needs a BR IP. |
| **RJ** | Thin. `dadosabertos.rj.gov.br` has 1,119 packages but the fiscal content is a per-agency PDF dump. The one real series is SEFAZ's `tfe-despesa` (`despesa_generica<ano>.csv`, 2016-2025, ~10 MB/yr) — a **month × budget-line aggregate**, `despesa_mensal` grain, no creditor, no empenho. Five preamble lines before the header; 2019 and 2021 each published twice; TLS drops mid-download. |
| **PB** | Usable, cheapest in the set, no BR IP. REST API at `api.dados.pb.gov.br/api/v1`, `ano`+`mes` required, ~2M rows, 2015+. Complete empenho→liquidação→pagamento chain plus `codigoMunicipio`. Tenders are winner-only. |
| DF, GO, PR, SE, RN, AM, MS, MA, PA, PI, TO | Portals answer; **no bulk fiscal files**. DF is Liferay; GO an Angular SPA on a WSO2 gateway; PR's `www.dados.pr.gov.br` is a Drupal brochure with no dataset content type (the real data is still the `jsessionid` JSF portal); PI, TO and GO serve the same page for every path. SP-SIGEO-class effort each. |
| MT, AC | Dead ends. MT has 26 packages and nothing fiscal; AC has 20 and one per-capita health indicator. |
| AL, AP, RR | No candidate host completes a TLS handshake, from Brazil either. Broken, not geo-blocked. |
