# Onboarding Plan — `au_abs_labour_force`, Detailed release (phase 2)

**Dataset:** `au_abs_labour_force` (existing, org `abs`) — **extended**, not duplicated.
**Source:** Labour Force, Australia, **Detailed** (ABS cat. 6291.0.55.001), final release
`.../labour-force-australia-detailed/mar-2026/<CUBE>.xlsx`
**License:** CC BY 4.0 (ABS standard)
**Scope:** all 15 data cubes of the ceased release → 12 new tables, ~925k rows.

This is the phase 2 deferred by `ONBOARDING_PLAN.md` §Scope ("The separate *Detailed*
release (6291.0.55.001: industry/occupation/duration cubes) is a deferred phase 2").

---

## 1. Why these tables go in `au_abs_labour_force`, not a new dataset

The ABS is deleting the distinction. Under *Modernising the Labour Force Survey*:

- The March 2026 reference period (published 23 April 2026) was the **final** release of
  *Labour Force, Australia, Detailed*. The page reads "This release has Ceased."
- The surviving tables move into **`Labour Force, Australia`** — the release this dataset
  already tracks — from the **September 2026 reference period, first published
  15 October 2026**.
- ABS is renumbering them into that release's own sequence: EQ06 → Table 021,
  EQ03 → IAO1, EQ12 → IAO2, EQ13 → IAO3, EQ07b → IAO4, EQ09 → IAO5, EQ05 → SEM3,
  EQ04 → SEM2, UM2 → UDJ1, UM3 → UDJ2, NM1 → NLF2, NM2 → NLF1.

A separate `au_abs_labour_force_detailed` dataset would enshrine a publication boundary
that stops existing, and its pipeline would point at a release page that no longer
publishes. Survey, organization, licence, geography vocabulary and sex vocabulary are
identical to the headline release.

**Cadence** differs today (quarterly industry/occupation vs monthly headline) and is not
an obstacle: tier and coverage are per-table properties in Data Basis, and this repo
already ships mixed-cadence datasets (`us_bls_cpi`: monthly + semiannual + annual).

---

## 2. What changes at the September 2026 reference period

Recorded here because it dictates the architecture, and because the successor tables are
**out of scope for this PR** (their names, URLs and code lists are unconfirmed until
15 October 2026).

| Change | Consequence for this design |
|---|---|
| Industry and occupation become **monthly**; history stays **quarterly**. ABS will not convert history to monthly, "due to limited indicators of historical monthly seasonality". | The tables here are quarterly and must stay quarterly. ABS will itself name the successors `Table 021_MTH` / `Table 021_QTR`. Do **not** upsample. |
| Occupation classification switches **ANZSCO (2013) v1.2 → OSCA**. | Occupation columns here carry `occupation_classification = 'ANZSCO (2013) v1.2'` so the future OSCA rows are distinguishable in the same column. |
| Industry stays **ANZSIC (2006) Rev.2.0**. | No break expected on the industry side. |
| Monthly seasonally adjusted industry data will **not** be published initially. | Nothing here is seasonally adjusted; every cube is Original. No `adjustment_type` column. |

---

## 3. Vocabulary — match the sibling tables per dimension, not blanket-lowercase

Read from the live prod table `basedosdados.au_abs_labour_force.labour_force_status`.
The existing dataset is **not** uniformly lowercase:

| Dimension | Case | Values |
|---|---|---|
| `sex` | lowercase | `males`, `females`, `persons` |
| `adjustment_type` | lowercase | `original`, `seasonally_adjusted`, `trend` |
| `status_in_employment` | lowercase | `employee`, `owner manager of …` |
| `geography` | **Title Case** | `Australia`, `New South Wales`, … |
| `age_group` | Sentence case + lowercase `total` | `15-19 years`, `65 years and over`, `total` |
| `hours_band` | Sentence case + lowercase `total` | `1-9 hours`, `Worked fewer than 35 hours`, `total` |

Blanket-lowercasing would break the join on `geography`, `age_group` and `hours_band`.

### Three vocabulary caveats to record in `observations`

1. **`sex` has no `persons` in these cubes** — only `Males` and `Females`. Do **not**
   synthesise a `persons` total by summing: ABS estimates are independently benchmarked
   and rounded, so the sum is not the published total.
2. **`status_in_employment` is finer here than in the sibling tables.** The cubes carry 8
   values including `employee with paid leave entitlements`, `employee without paid leave
   entitlements` and `employee, nfd`, where `status_in_employment` (headline) carries a
   single rolled-up `employee`. The vocabularies overlap but are not identical; joining on
   this column requires the rollup.
3. **`age_group` uses two different band sets** — six broad bands (`15-24 years` …
   `65 years and over`) in EQ07a/UM3, eleven five-year bands (`15-19 years` …) in
   EQ12/EQ13. Both sets already exist in the sibling `age_group` vocabulary, so both join.

---

## 4. Classification codes: split code from label where the source carries one

The user requirement is that industry/occupation codes be STRING and kept separate from
the label. The source ships codes only in one place:

- **EQ06 industry group labels are code-prefixed** — `010 Agriculture nfd`,
  `011 Nursery and Floriculture Production`. Split into `industry_group_code` (`'010'`,
  STRING — leading zero is significant) and `industry_group` (the label). 291 groups.
- **Industry division labels carry no code.** Attach the ANZSIC 2006 division letter
  (`A`–`S`) from a 19-row map hardcoded in the cleaning code, asserted to cover exactly
  the 19 observed labels and to fail loudly otherwise.
- **Occupation major group labels carry no code.** Attach the ANZSCO major code (`1`–`8`)
  from an 8-row map, asserted the same way.
- **Occupation sub-major (51 values) gets no code** — the 2-digit correspondence includes
  `nfd` residuals that are not mechanically derivable from the label. Label only; noted in
  `observations`.

`covered_by_dictionary = no` throughout, and **no `dicionario` table**. Per
`data-basis-style.md`, the flag is `yes` only when the *stored values are opaque codes*;
these are readable labels. This matches the sibling tables, which decode to labels and
ship no dictionary.

---

## 5. Series breaks flagged in the source

ABS has **already backcast** the classifications: every cube carries a single
classification across its whole history (`ANZSIC (2006) Rev.2.0` from 1984-11,
`ANZSCO (2013) v1.2` from 1986-08). There is no within-file industry revision break to
reconstruct.

The real breaks are flagged inline by ABS as **bracketed period suffixes on category
labels**, and must be preserved verbatim rather than cleaned away:

- `EQ03` geography: `Sydney (old standards) [1960-1991]` alongside `Greater Sydney` — the
  ASGS boundary change. 12 of the 26 GCCSA values are `(old standards)`.
- `EQ02` tenure: `12 months or more [2001-2014]` alongside the finer post-2014 bands.

Both are recorded in the affected column's `observations`, and the labels are kept as-is.

---

## 6. Table architecture — 12 tables, ~925k rows

Every cube has a clean long `Data 1` fact sheet with a uniform measure block, so the shape
is **wide-by-measure**, matching the sibling tables. Measure columns, converted from ABS
thousands to absolute units (`× 1000`) exactly as the sibling tables do:

| Column | Unit |
|---|---|
| `employed_full_time`, `employed_part_time` | `person` |
| `hours_worked_full_time`, `hours_worked_part_time` | `hour` |

`unemployment_duration` instead carries `unemployed_total`, `unemployed_looked_for_full_time`,
`unemployed_looked_for_part_time` (`person`) and `weeks_searching` (`week`).

### Industry

| Table | Cube | Grain | Rows | Coverage |
|---|---|---|---|---|
| `employment_industry` | EQ06 | quarter × state × industry_group × sex | 514,879 | 1984-11 → 2026-02 |
| `employment_industry_region` | EQ03 | quarter × gccsa × industry_division × sex | 87,786 | 1984-11 → 2026-02 |
| `employment_industry_age` | EQ12 | quarter × age_group × industry_division | 34,667 | 1984-11 → 2026-02 |
| `employment_industry_hours` | EQ10+EQ11+EQ14 | quarter × hours_measure × hours_band × industry_division | 51,437 | 2001-05 → 2026-02 |
| `employment_industry_status` | EQ05 | quarter × status_in_employment × industry_division | 16,625 | 1991-02 → 2026-02 |
| `employment_industry_occupation` | EQ09 | quarter × sex × industry_division × occupation_major | 47,795 | 1986-08 → 2026-02 |

### Occupation

| Table | Cube | Grain | Rows | Coverage |
|---|---|---|---|---|
| `employment_occupation` | EQ07a | quarter × sex × age_group × occupation_sub_major | 86,762 | 1986-08 → 2026-02 |
| `employment_occupation_age` | EQ13 | quarter × age_group × occupation_major | 13,992 | 1986-08 → 2026-02 |
| `employment_occupation_status` | EQ07b | quarter × status_in_employment × occupation_major | 7,136 | 1991-02 → 2026-02 |

### Other

| Table | Cube | Grain | Rows | Coverage |
|---|---|---|---|---|
| `employment_status_hours` | EQ04 | quarter × sex × status_in_employment × hours_band | 19,251 | 1991-02 → 2026-02 |
| `employment_job_tenure` | EQ02 | quarter × sex × state × tenure_band | 9,408 | 2001-05 → 2026-02 |
| `unemployment_duration` | UM2+UM3 | **month** × duration × state / age_group | 34,982 | 1991-01 → 2026-03 |

### The two consolidations, and why only these two

- **`employment_industry_hours` ← EQ10 + EQ11 + EQ14.** Identical grain and identical
  measure block; the three cubes differ only in *which hours concept* is banded. Carried as
  an `hours_measure` dimension ∈ {`hours usually worked in all jobs`, `hours actually
  worked in main job`, `hours usually worked in main job`}. The four distinct zero-labels
  (`Did not work (0 hours)`, `Usually doesn't work (0 hours)`, `Did not work in main job
  (0 hours)`, `Usually doesn't work in main job (0 hours)`) normalise to a single
  `Did not work (0 hours)` because `hours_measure` now carries the concept.
- **`unemployment_duration` ← UM2 + UM3.** Same measures, same monthly periods, one
  alternating breakdown each (state, age). Merged with the `total` sentinel in the
  unused dimension — the established house pattern, identical to how the sibling
  `labour_force_status` merges `LF` (state, age total) with `LF_AGES` (national, by age).

Everything else stays one table per cube. Merging cubes that share a dimension but survey
different orthogonal breakdowns (e.g. EQ12 age×division with EQ05 status×division) would
produce a table where summing across rows silently double-counts. Grain-faithful
separation is self-documenting and safe.

---

## 7. Column conventions

English column names throughout (this is an English dataset), `_id`-suffix rule not
triggered since no column is an identifier into a directory.

- Partition: **`year` (INT64)**. Range `start=1984, end=2031, interval=1`.
- `quarter` (INT64, 1–4) and `month` (INT64, 1–12) for the quarterly tables — the ABS
  "Mid-quarter month" is Feb/May/Aug/Nov, so `month` is retained rather than discarded and
  `quarter` derived from it. `unemployment_duration` is monthly and carries `month` only.
- `geography` (STRING, Title Case) where the cube is state-level, matching the sibling
  vocabulary; `gccsa` (STRING) in `employment_industry_region`.
- Ordering per `data-basis-style.md`: partition → temporal → geographic → other
  identifiers/codes → descriptive → measures.
- No `is_primary_key` anywhere (not a directory dataset).
- Every numeric column carries a `measurement_unit`; every categorical column is STRING.

### Directory links

`br_bd_diretorios_au` has no industry or occupation table, and the sibling tables already
leave `geography` unlinked pending that directory (`labour_force_status.geography` has
`directory_column` empty with the intent noted in `observations`). These tables follow the
same treatment: `year` → `br_bd_diretorios_data_tempo.ano:ano`, `month` →
`br_bd_diretorios_data_tempo.mes:mes`, geography noted but unlinked. ANZSIC/ANZSCO
directories are flagged in `observations` as future work, per the shared-entity rule.

---

## 8. Coverage tier

All 12 tables are **`AllFree`**. The BD Pro rolling window applies to tables refreshed
monthly or more often; these are a **frozen archive of a ceased release** with no pipeline,
so nothing rolls. (The sibling monthly tables keep their `PartBdpro` tier — untouched.)

---

## 9. Out of scope for this PR

- **No recurring pipeline.** The source is ceased; there is nothing to poll. The successor
  monthly tables arrive 15 October 2026 and are a separate PR, at which point the existing
  `au_abs_labour_force` flow gains the new table ids.
- **No changes to `pipelines/datasets/au_abs_labour_force/`.** Verified safe: the flow
  iterates `constants.DATA_TABLES` and calls `run_dbt(table_id=<one table>)`, so it neither
  builds nor overwrites the 12 new models. Per the scoped-change rule, the pipeline is not
  touched.
- **Job mobility.** There is no job-mobility cube in this release. `EQ02` is job *tenure*
  (months with current employer). True job mobility is ABS cat. 6209.0 *Participation, Job
  Search and Mobility*, a separate annual release — its own onboard if wanted.
- **The monthly cubes not listed above** (LM1–LM9, EM1–EM6, FM1–FM4, NM1–NM2, MRM1–MRM2,
  LQ1–LQ2). These are monthly *headline-adjacent* breakdowns rather than the
  industry/occupation gap this phase targets; several overlap the sibling tables' pivots
  (LMS1–5, MLF1, HRS1–2, SEM1) already onboarded from the headline release.

---

## 10. Step sequence

Standard 11-step onboarding, stopping at the verification checkpoint before prod:

1. architecture — 12 CSVs under `models/au_abs_labour_force/code/architecture_detailed/`
2. download — 15 cubes to `~/Downloads/au_abs_labour_force_data/input/`
3. clean — `models/au_abs_labour_force/code/clean_detailed.py` → partitioned parquet
4. upload — `basedosdados-dev`, all-STRING staging parquet via arrow
5. dbt — 12 models + `schema.yml` entries in the existing `models/au_abs_labour_force/`
6. validate — `dbt run` + `dbt test` for all 12
7. discover / metadata (dev) — extend the existing dataset record, 12 new tables
8. **checkpoint** — human approval
9. metadata (prod) → PR → merge → table-approve → verify → dataset stays `published`

The dataset is already `published` in prod; it stays published, with its description
refreshed for the extended coverage. New tables are registered `status.published` and go
live when the merge materialises them.

---

## 11. Addendum — EQ06's industry column, verified 2026-09-10

Two findings that **supersede §4's hardcoded-map instruction** for the ANZSIC division
letter. Both were verified empirically against the extracted category lists.

### EQ06 mixes two ANZSIC levels in one column (291 = 272 + 19)

- **272 values are 3-digit group codes** — `010 Agriculture nfd`,
  `011 Nursery and Floriculture Production`, … `969 …`.
- **19 values are division-level `nfd` residuals** coded as the division letter plus `00`
  — `A00 Agriculture, Forestry and Fishing nfd` … `S00 Other Services nfd`.

The 19 are legitimate residual categories (respondents whose industry could not be coded
to group level), **not** duplicates and **not** subtotals of the group rows. Summing all
291 gives the correct total: keep them, and do not treat them as an aggregate to exclude.

Consequences: `industry_group_code` must be **STRING** (it holds both `010` and `A00`),
and `observations` must state that the column carries group-level codes plus 19
division-level residuals.

### Derive the division map from the data — do not hardcode it

Parsing the letter from EQ06's `[A-S]00` rows and stripping the trailing ` nfd` yields
**exactly 19 letters A–S**, whose labels equal the 19 division labels observed in
EQ03/EQ05/EQ09/EQ10/EQ11/EQ12/EQ14 — set equality, zero missing, zero extra. Build the
map that way and **assert** both the count and the set equality, raising otherwise. This
beats a hardcoded map: no external source, no drift, and it self-validates against seven
other cubes.

Map the 272 group rows to a division by the ANZSIC 2006 subdivision (first two digits).
Verified: **all 272 fall inside these ranges, zero orphans.**

```
A 01-05   B 06-10   C 11-25   D 26-29   E 30-32   F 33-38   G 39-43
H 44-45   I 46-53   J 54-60   K 62-64   L 66-67   M 69-70   N 72-73
O 75-77   P 80-82   Q 84-87   R 89-92   S 94-96
```

**Populate `industry_division_code` (A–S) and `industry_division` (label) on every row of
`employment_industry`** — from the letter directly for the 19 residual rows, from the
subdivision range for the 272 group rows — so the table carries a complete rollup key
alongside `industry_group_code` / `industry_group`.

### ANZSCO majors — hardcoding is fine, confirmed against the official classification

The 8 labels in EQ07b/EQ09/EQ13 match the official ANZSCO major groups exactly:
`1 Managers · 2 Professionals · 3 Technicians and Trades Workers · 4 Community and
Personal Service Workers · 5 Clerical and Administrative Workers · 6 Sales Workers ·
7 Machinery Operators and Drivers · 8 Labourers`. An 8-entry map with a full-coverage
assertion is correct.

Occupation columns carry their own `… nfd` residuals (`Managers nfd`, `Professionals
nfd`, `Clerical and Administrative Workers nfd` among EQ07a's 51 sub-major values) —
same treatment as industry: real categories, keep them.
