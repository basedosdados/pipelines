# us_epa_tri

EPA Toxics Release Inventory (TRI): annual facility-level reporting of toxic
chemical releases, off-site transfers and waste management under EPCRA
section 313, reporting years 1987 to present. Source: the "TRI Basic Data
Files" (one national CSV per year, 122 columns, one row per Form R / Form A
filed by a facility for one chemical), generated on the fly by Envirofacts.

## Refresh cadence
- `7 4 * * 3` — Wednesdays 04:07 America/Sao_Paulo. The page poll no-ops until
  EPA's "processed as of" date moves (preliminary dataset in July, final
  National Analysis dataset in October/November, plus regenerations).
- Each real run re-downloads the **two newest reporting years** and rewrites
  those `year=` partitions (`dump_mode="append"`, so older partitions stay).

Staging upload: dump mode `append`, source format `parquet`.
Worker sizing: `"memory": "4Gi"`.

## Tables
| table | grain | partition | coverage tier | columns |
|---|---|---|---|---|
| `facility` | year × TRIFID | `year` | all_free | 21 |
| `chemical` | year × TRI chemical id | `year` | all_free | 12 |
| `form` | year × document control number | `year` | all_free | 46 |
| `release` | year × document control number × release category (nonzero only) | `year` | all_free | 8 |

`chemical` is partitioned like the data tables on purpose: a run that refreshes
only the newest reporting years rewrites those partitions, whereas an
unpartitioned dimension would be replaced wholesale and would lose every
chemical last reported in an earlier year. `assert_output_layout` fails the
clean if a file from an earlier layout is still in an output directory, because
the upload ships the whole directory.
| `dicionario` | — | — | — | 5 |

## Where the code lives
- `pipelines/datasets/us_epa_tri/` — `constants.py` (URLs, table list, NAICS
  vintages), `utils.py` (pure download + cleaning transform, the release
  category spec, the dicionario spec), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + inline schedule).
- `models/us_epa_tri/` — dbt models and `schema.yml`, both **generated** from
  `code/architecture/*.csv` by `code/build_dbt_models.py` and
  `code/build_schema_yml.py`. The architecture CSVs and `code/columns_json/`
  are generated from `code/build_architecture.py`. Edit the generator, then
  regenerate — do not hand-edit the outputs. `code/dicionario.csv` is written by
  `code/build_dicionario.py` from the spec in `utils.py`.
- Scratch data: `~/Downloads/us_epa_tri_data/{input,ref,output}` (never in the
  repo or under Dropbox).

## Source
- https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present
- Per-year national file: `https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/<year>_US/csv`
  (~60 MB, streamed at ~150 KB/s per connection; an interrupted stream still
  returns HTTP 200, so a file is accepted only when its last row has 122 fields).
- County FIPS per facility: Envirofacts `tri_facility` (`state_county_fips_code`),
  `https://data.epa.gov/efservice/tri_facility/rows/<a>:<b>/JSON`, 10,000 rows a page.
- Documentation: "TRI Basic Data Files Documentation" (August 2024),
  https://www.epa.gov/system/files/documents/2025-09/basic_data_files_documentation_august_2024.pdf
- US Government work, public domain (17 USC §105); registered as `cc0`.

## Design notes
- **The form, not the facility, is the unit.** `DOC_CTRL_NUM` is unique per
  year; (facility, chemical) is not (694 repeats in 1987 alone). SIC/NAICS codes
  vary across a facility's forms within a year (EPA assigned NAICS per
  submission through 2005), so they live on `form`, not `facility`. Every other
  facility attribute is constant within (year, TRIFID) — checked at clean time.
- **Units.** Dioxins are reported in grams, everything else in pounds. Every
  quantity column is in one unit: `quantity_pounds` and the `form` totals are
  pounds for every row (grams / 453.59237), and `release.quantity_grams` keeps
  the dioxin rows as reported.
- **`release` is long and sparse.** Only the 55 leaf columns of sections 5, 6.1
  and 6.2 are unpivoted; EPA's totals (on-site, POTW, off-site release/recycling/
  energy recovery/treatment, total transfer, total releases) and section 8 stay
  wide on `form`. Zero rows are dropped: the source fills zeros for NA, blank and
  Form A. Category columns are era-exclusive (5.4 → 5.4.1/5.4.2 in 1996, 5.5.1
  → A/B in 1996, 5.5.3 → A/B in 2003, M71/M72/M63 retired), so summing a form's
  rows reproduces on-site total + total transfer; EPA's own totals disagree with
  their parts on ~1% of 1987 forms (POTW treatment left out of the treated
  total), and are copied as published.
- **NAICS vintage** is derived from the year (`naics_version`) and was measured,
  not assumed: TRI adopts each Census revision one year late (RY 2012 codes are
  NAICS 2007). Each vintage is FK-tested against its
  `br_bd_diretorios_us.naics_<vintage>` directory with a 2% tolerance. `state`
  links to the directory's `abbreviation`; `county_id` (from Envirofacts) to
  `county.id_county` with a 2% tolerance (Connecticut still reports the legacy
  county FIPS the directory replaced with planning regions).
- **Source sentinels mapped to null:** SIC `INVA`/`NA`, county FIPS `00000`.
  `HORIZONTAL DATUM` is empty in every row and is not loaded.
- Output is **all-STRING** hive-partitioned parquet; the dbt models `safe_cast`
  every column. `year` is in the path only. The bootstrap writes a 0-row
  `00_header.parquet` per partition (table-approve OOM guard); the recurring
  pipeline must not (its `dump_header` would infer INT64 from an empty frame).
- Every table is `dbt run` before any is `dbt test`ed: `custom_dictionary_coverage`
  reads the sibling `dicionario` model.
- All tables are `AllFree` (annual data; the BD Pro rolling window applies to
  monthly-or-faster tables only).
