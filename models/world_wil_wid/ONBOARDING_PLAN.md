# WID (World Inequality Database) — onboarding plan

Source: https://wid.world/data/ → "download full dataset"
Bulk URL: `https://wid.world/bulk_download/wid_all_data.zip` (882 MB zip → 6.4 GB, 848 CSVs)
Snapshot inspected: `Last-Modified: 2026-07-25`; member files dated 2026-07-26.

## 1. What the source actually is

Three file families inside one zip, all `;`-delimited:

| File | Count | Grain |
|---|---|---|
| `WID_data_<GEO>.csv` | 422 | country × variable × percentile × year |
| `WID_metadata_<GEO>.csv` | 422 | country × variable (labels, unit, source, method) |
| `WID_countries.csv` | 1 | geography dimension (incomplete — see trap 4) |

Measured totals (all files, read with `union_by_name`):

- **fact rows: 142,366,460** — 410 geographies, 2,486 variables, 809 percentiles, years **1800–2025**
- **metadata rows: 395,977** — key `(country, variable)` is unique
- `WID_countries.csv`: 346 rows

Geography mix: 235 countries · 61 subnational (`US-CA`, `DE-BY`, `CN-UR`) ·
49 regional aggregates (`WO` = World, `O*`, `Q*`, `X*`) · 43 `-MER` · 22 `-PPP` variants.
The MER/PPP suffix is part of the `country` code itself, not a separate column.

Row mix by percentile: 92.6M bracket `pXpY` · 25.0M `p0p100` totals ·
24.7M fractional top-detail (`p99.9p100`) · 19k single `pX`.
Row mix by series type: `a` average 42.1M · `s` share 35.5M · `t` threshold 34.2M ·
`b` 11.2M · `m` total 4.1M · then 10 smaller types.
Top concepts: `ptinc` 42.0M · `hweal` 36.2M · `diinc` 33.6M.

## 2. Traps found (each one silently corrupts a naive load)

Traps 1, 2, 4 and 5 all fail *without raising*. Each was hit for real during this
onboarding, not anticipated on paper.

1. **The `Al` stubs destroy Albania — both its data and its metadata.** The archive
   carries `WID_data_Al.csv` (47 bytes) and `WID_metadata_Al.csv` (168 bytes), header-only
   artifacts dated 2024-02-14, whose names differ from Albania's real `WID_data_AL.csv`
   (19.7 MB) and `WID_metadata_AL.csv` (2.1 MB) only by case. On macOS/APFS
   (case-insensitive) `unzip` overwrites both with the stubs and Albania vanishes. Read
   members from the zip via `zipfile` and skip the two stubs by name. Measured cost of
   missing it: 1,321 series and ~454k fact rows silently absent.
2. **76 of 422 data files have a 7-column header with no `data_quality`** — exactly the
   region aggregates (`WO`, `O*`, `Q*`, `X*`). Their metadata files likewise drop
   `data_quality_score`. Reading with a fixed 8-column schema plus `ignore_errors=true`
   silently discards all 36.6M aggregate rows, **World included**. Use `union_by_name`.
3. **Variable code order in the bulk CSVs is `[type][concept5][pop][age3]`** — `sptincj992`,
   `accmhni992` — not the `[type][concept][age][pop]` order the site's codes dictionary
   documents. `age` and `pop` are already separate columns; use those, do not slice
   positionally without checking.
4. **`WID_countries.csv` is missing 67 of 410 geographies** — every regional aggregate,
   World included. Recover their names from the `countryname` column in the metadata files.
5. **A default CSV null-sentinel list deletes Namibia.** pyarrow's `strings_can_be_null`
   defaults to treating `NA`, `N/A`, `null`, `nan` and friends as null, so Namibia's ISO
   code `NA` parses as NULL in `country` — 1,151 series and every Namibian fact row lose
   their geography with no error raised. pandas' `read_csv` has the same default. Pass an
   explicit `null_values=[""]`. The cleaning code additionally asserts that no key column
   holds a null, so this class of failure cannot recur quietly.

6. `source` / `method` fields carry `[URL][URL_LINK]…[/URL_LINK][URL_TEXT]…[/URL_TEXT][/URL]`
   pseudo-markup, in either tag order, and embedded `;` and newlines inside quotes. Some
   blocks butt straight up against the next word, since WID's own rendering relies on the
   anchor tag for separation.

7. Minor: `region2` carries inconsistent spellings for the same sub-region
   (`Eastern_Africa` vs `East Africa`, `Eastern_Europe` vs `Eastern Europe`), preserved
   as published. `data_quality` is integer 0–5 except for 46 Chilean rows in
   `wpwodki999` that carry fractional values — kept as they come, which is why the
   column is STRING.

## 3. Proposed architecture — 3 tables plus a dictionary

Mirrors the source grain. Same shape as `us_fed_fred` (`observation` + `series`), which
solves the same problem: one `value` column whose unit varies by series.

### `indicator` — 142.4M rows (fact)
`year` INT64 (partition, 1800–2030) · `country_code` · `variable` · `series_type` (derived,
1 char) · `concept` (derived, 5 chars) · `pop` · `age` · `percentile` · `value` FLOAT64 ·
`data_quality` STRING (0–5 code, NULL on aggregates).
Cluster on `country_code, concept, percentile`. `value` carries no fixed
`measurement_unit` — the unit lives on `series.unit` (FRED precedent).

### `series` — 395,977 rows (metadata dimension)
`country_code` · `variable` · `age` · `pop` · `country_name` · `name` · `simple_description` ·
`technical_description` · `short_type` / `long_type` · `short_pop` / `long_pop` ·
`short_age` / `long_age` · `unit` · `source` · `method` · `extrapolation` · `data_points` ·
`data_quality_score`.

### `country` — ~413 rows (geography dimension)
`country_code` · `title_name` · `short_name` · `region` · `region2` ·
`geography_type` (country / subnational / aggregate, derived) ·
`conversion` (MER / PPP / none, derived) · `country_iso2` (nullable, FK to
`br_bd_diretorios_mundo.pais` for real countries only).
Built by unioning `WID_countries.csv` with the 67 aggregates recovered from metadata.

### `dicionario`
Value→label for `data_quality` (0–5), `pop` (i/j/t/f/m), `age` (999/992/996/…),
`series_type` (a/s/t/m/g/b/…).

**Not split by aggregation level.** "Macro vs distributional", "country vs region",
"MER vs PPP" are filter predicates on `percentile` / `series_type` / `country_code`.
Splitting them into separate tables invents a taxonomy the source does not have and
breaks the single `(country, variable)` join to `series`.

Column names in **English** (`year`, not `ano`; `_id` suffix convention) — the source is
English-language and international.

## 4. Pipeline

- **Cadence:** WID states series are "continuously updated"; in practice the bulk file is
  rebuilt a few times a year, irregularly (Our World in Data re-snapshotted 2024-05,
  2025-03, 2026-01, 2026-06). There is no published schedule.
- **Poll signal:** `HEAD https://wid.world/bulk_download/wid_all_data.zip` exposes
  `Last-Modified` and `ETag`. Poll weekly, compare against the table update
  (`compare_against="table_update"`, the `au_ato_abr` pattern), rebuild on change.
- **Mode:** the source ships full history every release → `dump_mode="overwrite"`,
  full rebuild. No incremental logic, no snapshot stacking.
- **BD Pro:** refresh is a few times a year, well under monthly → **`AllFree`** on every
  table. No rolling window, no Row Access Policies.

## 5. Decisions taken

1. **Dataset id `world_wil_wid`**, org slug `wil` (World Inequality Lab), backend dataset
   slug `wid`. Neither the org nor the dataset existed in the backend; both are created
   by this onboarding.
2. **License CC BY 4.0.** WID publishes no first-party license page — the site says only
   "open access". Our World in Data's ETL records WID as CC BY 4.0 consistently across
   four snapshots (2024-05 → 2026-06). That is third-party attribution, not a first-party
   grant, and is recorded here as the basis for the choice.
3. **`source` / `method` markup stripped to plain text** plus the URL in parentheses.
4. **`series_type` and `concept` live on the fact table** as well as on `series`, so a
   query can filter by concept without string surgery and BigQuery can cluster on it.

## 6. Open items

- `data_quality` (0–5) has no published WID codebook, so it is registered as an
  undocumented code rather than given invented labels. Worth asking WID directly.
- `country_iso2` carries no directory foreign key. WID's geography codes are ISO 3166-1
  alpha-2 only for the 242 current countries; the other 171 are subnational units,
  WID regional aggregates, and historical entities on ISO 3166-3 or user-assigned codes
  (`SU`, `YU`, `CS`, `DD`, `XC`). An unresolved `directory_column` gets the whole column
  dropped at `upload_columns_from_sheet`, so the link is deliberately not declared until
  the `br_bd_diretorios_mundo.pais` primary key is confirmed to accept `sigla_iso2`.
