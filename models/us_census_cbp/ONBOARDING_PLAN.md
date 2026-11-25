# us_census_cbp — Onboarding Plan (architecture spec)

County Business Patterns (CBP), U.S. Census Bureau. Establishments, employment,
and payroll by geography × industry (NAICS) × legal form × establishment-size,
annual. Status: **onboarded** — data in staging + prod metadata, PR #1691.

- GCP dataset id: `us_census_cbp` · org: `us_census` · backend slug: `cbp`
- License: **public domain** (U.S. Government work, 17 U.S.C. §105; data.gov tags CC0)
- Landing page: https://www.census.gov/programs-surveys/cbp.html
- Downloads: https://www.census.gov/programs-surveys/cbp/data/datasets.html
- Raw file pattern: `https://www2.census.gov/programs-surveys/cbp/datasets/<year>/cbp<yy>{us,st,co}.zip`

## Decisions locked (user, 2026-07-16)

1. **Tables in English**, metadata trilingual (PT/EN/ES).
2. **Keys id-first**: `id_state`, `id_county`, `naics` — matches `br_bd_diretorios_us`
   PKs (`id_state`/`id_county`/`id_naics`) and BD-wide convention. (User initially
   asked id-last; reversed after seeing the directory is id-first.)
3. **Full history + per-vintage NAICS directories.** Keep all NAICS-era years
   (1998–2023). `naics` is one STRING column whose directory FK is
   `naics_2017` (the vintage CBP publishes for the most recent years);
   referential integrity is enforced per-vintage in dbt via `naics_version`
   against all five vintage directories.
4. **Include US/state size-class $ + lfo detail** (not just totals).
5. **Payroll in whole USD** (source is $1,000s → multiply by 1000; unit = USD).
6. **Shape: WIDE** — one row per source row (geo × naics × lfo); establishment-size
   bands are columns. Reversed from an initial LONG design once grounded row counts
   showed LONG explodes county to ~15M rows/yr (≈150–400M total, ~90% null $), while
   WIDE is ~38M total and is the canonical CBP format. national/state ~85 cols
   (mechanical size-class × measure grid), county ~26.

## Source structure (measured from raw files)

Annual flat CSVs, one per geography. Two schemas:

- **US (`cbpNNus`) & State (`cbpNNst`)** — grain `naics × lfo`, with employment &
  payroll broken out **per establishment-size-class** and per **lfo**. Columns:
  `fipstate?, naics, lfo, empflag, emp_nf, emp, qp1_nf, qp1, ap_nf, ap, est`, then
  for each size band S∈{1_4,5_9,10_19,20_49,50_99,100_249,250_499,500_999,1000}:
  `fS`(suppression flag), `eSnf,eS`(emp+noise), `qSnf,qS`(Q1 pay), `aSnf,aS`(ann pay), `nS`(establishments).
- **County (`cbpNNco`)** — grain `naics`, **no lfo, no per-size $**. Columns:
  `fipstate, fipscty, naics, empflag, emp_nf, emp, qp1_nf, qp1, ap_nf, ap, est`,
  establishment counts `n1_4…n1000` plus finer `n1000_1,n1000_2,n1000_3,n1000_4`
  (1000–1499 / 1500–2499 / 2500–4999 / 5000+), `censtate, cencty`(dropped).

### Two disclosure eras (harmonized via flags, not dropped)
- **1998–2016**: suppression. `empflag` = letter A–M marking withheld-cell size range; `emp`=0 when withheld.
- **2017–2023**: noise infusion. `emp` present; `emp_nf`/`qp1_nf`/`ap_nf` = G/H/J noise level, `D`=withheld.

### `lfo` — Legal Form of Organization (US/state only), dict-covered
Values observed: `-` (all), `C`, `S`, `P`, `N`, `O`, `Z`, `G`. Exact labels from CBP
reference docs → `dicionario`.

### NAICS vintages (the reason for per-vintage directories)

**CBP lags the NAICS revisions — there is no 2022 vintage in this data.** CBP still
publishes NAICS 2017 through 2023, so 2022/2023 rows carry `naics_version = 2017`.

| CBP years | NAICS vintage | `naics_version` | FK directory | unresolved vs its own dir | unresolved vs naics_2022 |
|---|---|---|---|---|---|
| 2017–2023 | 2017 | `2017` | `naics_2017` | 0.000% | n/a (516 codes absent from naics_2022) |
| 2012–2016 | 2012 | `2012` | `naics_2012` | 0.000% | 2.1% (43 codes) |
| 2008–2011 | 2007 | `2007` | `naics_2007` | 0.000% | 11.5% (246 codes) |
| 2003–2007 | 2002 | `2002` | `naics_2002` | 0.000% | larger |
| 1998–2002 | 1997 | `1997` | `naics_1997` | 0.094% (`95`/`99` only, ignored) | larger |

NAICS field in raw files carries aggregation fill: `------`(all-industry total),
`11----`(sector, dashes), `113///`/`1131//`/`11311/`(slashes), `113110`(6-digit).
**Cleaning:** strip dash/slash fill → native code; **drop** the all-industry `------`
total (derivable); keep all real 2–6-digit levels. Sector split-codes `31/32/33`,
`44/45`, `48/49` stay native and need **no test exception**: the per-vintage dirs are
built from CBP's own reference files and store `31`/`44`/`48` natively, unlike the
official `naics_2022` dir, which stores ranges (`31-33`). Verified zero unmatched
split-sector rows in every vintage.

## Tables

### 1. `national` — year × naics × lfo  (from `cbpNNus`) — 84 cols
### 2. `state` — year × id_state × naics × lfo  (from `cbpNNst`) — 85 cols
### 3. `county` — year × id_state × id_county × naics  (from `cbpNNco`) — 26 cols
### 4. `dicionario` — value→label for coded columns

WIDE: one row per source row; size bands are columns. Sheet rows generated by
`code/gen_arch_sheets.py` (single source of truth for the wide column grid).

Keys/dimensions (BD order): `year`(INT64, partition, FK ano), `id_state`(state,county),
`id_county`(county), `naics`(STRING, FK `naics_2017:id_naics` †), `naics_version`
(STRING, dict), `lfo`(STRING, dict — national & state only).

Total (all-size) measures, every table: `establishments`(est), `employment`(emp),
`employment_flag`(empflag, dict), `employment_noise_flag`(emp_nf, dict),
`payroll_first_quarter`(qp1×1000, USD), `payroll_first_quarter_noise_flag`(qp1_nf),
`payroll_annual`(ap×1000, USD), `payroll_annual_noise_flag`(ap_nf).

Per size-band columns:
- **national & state** — 8 columns per band S∈{1_4,5_9,10_19,20_49,50_99,100_249,
  250_499,500_999,1000}: `establishments_S`(nS), `employment_S`(eS),
  `employment_S_flag`(fS), `employment_S_noise_flag`(eSnf),
  `payroll_first_quarter_S`(qS×1000), `payroll_first_quarter_S_noise_flag`(qSnf),
  `payroll_annual_S`(aS×1000), `payroll_annual_S_noise_flag`(aSnf). = 72 cols.
- **county** — establishment COUNTS only (no per-band $): `establishments_S` for the
  9 bands (band `1_4` ← source `n<5`) plus finer 1,000+ bands `establishments_1000_1499`
  (n1000_1), `_1500_2499`(n1000_2), `_2500_4999`(n1000_3), `_5000_more`(n1000_4). = 13 cols.

Units: `establishment*`→establishment, `employment*`→employee, `payroll*`→USD (all INT64).
Flags/`lfo`/`naics_version` are dict-covered STRING. Header-driven cleaning fills any
column absent in a given era (`lfo` pre-2008, noise flags pre-2017) with null.

- † `naics` FK is declared against `naics_2017` (the vintage in force for the most
  recent years); per-vintage integrity via dbt (see below).

Logical key (dbt uniqueness): national `(year,naics,lfo)`; state
`(year,id_state,naics,lfo)`; county `(year,id_county,naics)`.
No `is_primary_key` in the backend (non-directory tables).

Est. rows (grounded on 2023): county ≈28.6M (1.10M/yr), state ≈9.1M (0.35M/yr),
national ≈0.3M (12k/yr) → ~38M total over 1998–2023.

## Prerequisite sub-project: NAICS vintage directories

Extend `br_bd_diretorios_us` with **five** new tables — `naics_1997, naics_2002,
naics_2007, naics_2012, naics_2017` — mirroring `naics_2022` structure (`id_naics,
name, level, id_sector, name_sector, id_subsector, …`).

**Key fact: CBP still uses NAICS 2017 through 2023** (2022/2023 data are NAICS 2017,
not 2022 — CBP lags the revision; verified — 516 NAICS-2017-only codes like 442/511/515
appear in 2022–2023 data and are absent from the official `naics_2022`). So CBP never
FKs to the official `naics_2022` directory.

Per-vintage FK mapping (`naics_version` → directory), each resolving ~exactly:
`1997→naics_1997` (0.09%), `2002→naics_2002` (0%), `2007→naics_2007` (0%),
`2012→naics_2012` (0%), `2017→naics_2017` (0%, covers 2017–2023).

**Source = CBP's own `naics-descriptions/` reference files** (exact code+title match
to the data; uniform format; sector split-codes 31/32/33 kept native, so tests are
clean for 2003–2016). Base:
`https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics<vintage>.txt`
(confirmed 200 for 2002/2007/2012/2017; `naics.txt` = current/2022). Codes carry
aggregation fill (`------`,`11----`,`113///`) → strip to native, drop `------`,
derive level + parent codes (clean_naics_2022 logic at
`models/br_bd_diretorios_us/code/clean.py:868`). Because these dirs come from CBP's
own files they store the sector split-codes `31`/`44`/`48` natively, unlike the
official `naics_2022` dir, which stores ranges (`31-33`) — so no split-code test
exception is needed for any vintage.

**NAICS 1997** (CBP 1998–2002): no CBP naics-descriptions file, but built from the
official **1997→2002 concordance** (`.../library/reference/naics/technical-documentation/concordance/1997_naics_to_2002_naics.xls`)
— authoritative 1997 6-digit code set, parents derived, titles from `naics_2002`
(shared levels) + each code's 2002 successor. Full official structure, so ~360 codes
are null-titled (industries CBP never tabulates: crop/animal 111/112, public admin 92;
and restructured construction 233/234/235). 1998–2002 data resolves at **0.094%**
(only admin codes `95`,`99`, ignored).

Building `naics_1997` was chosen over folding 1998–2002 into `naics_2002`, which left
11.2% unresolved (construction was restructured between the two vintages).

Add dbt models `br_bd_diretorios_us__naics_<v>.sql`, upload to staging, register
metadata, promote. Enables per-vintage `custom_relationships` tests below.

## dbt

- Model files `us_census_cbp__<table>.sql`, `safe_cast` every column, staging via
  `set_datalake_project("us_census_cbp_staging.<table>")`.
- Partition by `year` INT64, range {start:1998, end:2028, interval:1}.
- Industry integrity: **one `custom_relationships` test per vintage**, scoped by
  `naics_version`, each → `ref('br_bd_diretorios_us__naics_<v>')` on `id_naics`.
- Each vintage test runs at **zero** `proportion_allowed_failures` (measured 0.000%
  unresolved; 1997's `95`/`99` handled by `ignore_values`). A non-zero allowance
  would only mask a future unmapped code.
- `custom_relationships` → `ref('br_bd_diretorios_us__state')`/`__county` for geo.
- `dbt_utils.unique_combination_of_columns` on the logical key of **all three**
  tables, strict. The one duplicate key in county (the 1999 source file repeats a
  block for FIPS 01045, Dale County AL; 5 pairs are byte-identical and dropped by the
  cleaner's dedupe, NAICS 62111 has one genuinely conflicting pair kept faithfully)
  sits in the 1999 partition, and county's tests are scoped to the most recent
  partition, which has zero duplicates. Verified duplicate-free in all 25 other years.
- `not_null_proportion_multiple_columns` at **1.0** (zero nulls) with `ignore_values`
  for the per-band and flag columns, which are legitimately empty for most small
  cells / eras. The core columns measure 100.00% non-null across the full history of
  all three tables, so anything less than 1.0 would detect nothing.
- `dicionario` carries strict uniqueness on `(id_tabela, nome_coluna, chave)` plus
  `not_null` on all columns except `cobertura_temporal`.

### Test cost control (most-recent-partition scoping)
CBP's historical partitions are **immutable** — only the newest year gains data — so
re-scanning 48.7M county rows on every test run is waste.
- **`county` + `state`** (`BIG_TABLES`): every test is scoped with
  `config: {where: __most_recent_year_en__}` → tests touch one year's partition.
  Their `naics` FK collapses to the single current vintage (`naics_2017`).
- **`national`** (225k rows): **unscoped**. It is cheap *and* carries every NAICS code
  of every vintage, so it keeps the full 5-vintage FK battery and is the table that
  validates all of history.
- `__most_recent_year_en__` is an **English `year` variant** of BD's
  `__most_recent_year__` (which is hardcoded to `ano`), added additively to
  `macros/custom_get_where_subquery.sql` — reusable by other `us_*`/`world_*` datasets.
- `id_county` FK carries `proportion_allowed_failures: 0.02` (~1.2% unmatched): CBP's
  `999` statewide pseudo-county plus historical county vintages (pre-2022 Connecticut
  counties, dissolved Alaska boroughs, Dade County FL).

## Step sequence (per onboarding-workflow.md)

1. ✅ context / research (Eckert harmonized panel noted; raw Census chosen)
2. ✅ architecture sheets (4 CBP tables, WIDE) on Drive
3. ✅ download raw (us/st/co, 1998–2023) — US is `.txt` (older) or `.zip` (recent)
4. ✅ clean → partitioned parquet (header-driven wide; naics normalize; payroll ×1000)
5. ✅ **[sub-project]** build + upload **5** naics vintage directories
   (1997/2002/2007/2012/2017)
6. ✅ upload CBP parquet → BigQuery staging
7. ✅ dbt models + schema.yml (+ vintage tests)
8. ✅ validate
9. ✅ discover ids → metadata (staging)
10. ✅ [PAUSE checkpoint]
11. ✅ metadata prod → PR #1691

## Open items / risks

- **Download filename logic (resolved):** US file is `cbpNNus.txt` (uncompressed)
  for older years, `cbpNNus.zip` for recent years; county/state are always
  `cbpNN{co,st}.zip`. 1998 `cbp98us.txt` confirmed 200 (466 KB). Handle both in the
  downloader (try `.zip` then `.txt`).
- **NAICS 1997 source (resolved)** — built from the official 1997→2002 concordance;
  see Prerequisite. The earlier fallback of pointing 1998–2002 at `naics_2002` was
  rejected (11.2% unresolved).
- SIC era 1986–1997 deferred (incompatible classification) — possible future `*_sic`.
- Backend: use **staging** env per standing directive (dev down 2026-07-09); re-check.
- Eckert et al. imputed/harmonized panel (fpeckert.me/cbp, NBER WP 26632) is a
  possible future value-add table — third-party, needs attribution/terms check.
