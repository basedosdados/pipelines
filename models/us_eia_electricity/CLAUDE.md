# us_eia_electricity — design notes

U.S. Energy Information Administration **Forms EIA-860 and EIA-923**: the annual
inventory of every electricity generating plant and generator in the United
States, and the monthly report of what each plant generated, what fuel it burned,
and what that fuel cost delivered. Source, cleaning code and dbt models all live
under this directory and `pipelines/datasets/us_eia_electricity/`.

## Naming

The GCP dataset is **`us_eia_electricity`**, not `us_eia`. EIA publishes across
petroleum, natural gas, coal, nuclear, renewables and the state energy
consumption series (SEDS) as well as electricity; a single `us_eia` would have to
absorb all of them or permanently misname itself. The backend slug is
`electricity`, under the existing `eia` organization, which leaves
`us_eia_seds`, `us_eia_petroleum` and the rest free for later.

## Source

Two forms, one ZIP per report year each.

| form | what | url | years | tables |
|---|---|---|---|---|
| EIA-860 | annual plant and generator inventory | <https://www.eia.gov/electricity/data/eia860/> | 2001-2025 | `plant`, `generator` |
| EIA-923 | monthly generation, fuel and receipts | <https://www.eia.gov/electricity/data/eia923/> | 2001-2026 | `generation_fuel`, `fuel_receipts_costs` |

663 MB of source ZIPs in total. 2001 is the floor for both: before it the forms
are EIA-860A/860B and EIA-906, which are differently shaped surveys rather than
earlier vintages of the same one.

Licence: **US Government public domain.** EIA states that its publications are
not subject to copyright and that its data may be used and redistributed, asking
for an acknowledgement with the publication date
(<https://www.eia.gov/about/copyrights_reuse.php>). Registered as `cc0`, the
house mapping for a US federal source.

## PUDL: what is reused and what is not

The layout of these workbooks changes almost every year — files are renamed,
sheets are split and merged, header rows move, and columns are renamed. Reading
25 vintages by hand is archaeology, and the **Public Utility Data Liberation**
project (Catalyst Cooperative, MIT licence) has already done it.

`code/pudl/` holds its extraction maps and code vocabularies, vendored verbatim
by `code/vendor_pudl.py` from a pinned commit. Nothing there is edited by hand.

**Reused:**

- the (year, page) → file / sheet / header-rows maps;
- the (year, raw column) → canonical column maps, one per page;
- the code vocabularies, and the two mechanical repairs they carry: `code_fixes`
  (a dirty code standing for a real one — the EIA-860 status column sometimes
  holds the whole sentence `(OP) Operating` instead of `OP`) and `ignored_codes`
  (a code that means nothing, which becomes NULL);
- the two published-value repairs PUDL documents: EIA-923's bare `.` for NA, and
  its fuel cost being in **cents** per MMBtu.

**Not reused — PUDL's inferential transforms.** PUDL backfills the
`prime_mover_code` that the 2001 and 2002 EIA-923 files do not carry, from other
years; remaps municipal solid waste into biogenic and non-biogenic splits; and
aggregates rows that collide on the natural key. Those suit PUDL's harmonised
warehouse. Data Basis publishes the microdata as filed, so a value the respondent
did not report stays null here and colliding rows stay as two rows.

## The four traps

Each was measured over the whole 2001-2026 corpus, not assumed.
`code/verify_parquet.py` re-checks the ones that can regress.

### 1. EIA renames the current year's file every month

This is the trap that would have broken the pipeline within weeks, and it is not
an edge case — it is the normal state of the live year. EIA stamps both the
latest data month and the publication date into the EIA-923 file name:

```
EIA923_Schedules_2_3_4_5_M_05_2026_21JUL2026.xlsx     <- PUDL's vendored map
EIA923_Schedules_2_3_4_5_M_06_2026_21AUG2026.xlsx     <- what eia.gov served
```

The PUDL checkout used here was taken the day before this dataset was built and
its map was **already one release out of date**. EIA-860 does the same on an
annual clock, dropping `_Early_Release` when the final revision lands.

So the mapped name is a hint, not a contract. `YearReader._member` tries it
first — for the twenty-odd settled years it is exact — and falls back to a
per-page pattern in `constants.MEMBER_PATTERNS` matched against the ZIP's members.
A pattern matching no member, or more than one, raises rather than guessing.
`data_maturity` is then read off the name of the file that was actually opened,
because on the live year the two differ and the map's name is the stale one.

A related guard: a ZIP for a report year the vendored maps do not cover raises
instead of being silently skipped. That is what happens the first time EIA
publishes a new year, and skipping it would drop a year of data on a green run.

### 2. The layouts have moved since PUDL archived them

PUDL reads its own Zenodo archive of each original release; eia.gov has since
re-issued some archive ZIPs. A header-by-header scan of all 8 pages across all 51
ZIPs found exactly **one** divergence: the 2013 plant file PUDL recorded as
having `NERC Region Code` is served today with the header `NERC Region`.

That single case is recorded in `constants.MAP_OVERRIDES` with the evidence, and
the reader **raises** on any column the map expects but the file lacks, or any
column the file has but the map does not know. Both would otherwise appear
downstream as a column silently full of nulls.

### 3. `generation_fuel` is published wide, and its annual totals double-count

EIA-923 page 1 is one row per (plant, energy source, prime mover) carrying twelve
columns for each of six measures, plus an annual total per measure. Shipped as
published, the single most useful table in the dataset would be unusable in SQL.

It is melted to long here, one row per month, and the annual total columns are
**dropped** — they are the sum of the twelve monthly columns, and anyone summing
the table with them in it would double every number.

A (row, month) pair where all six measures are null is dropped, which is what
makes a partial year work: the current-year file carries twelve months of columns
but reports only through the latest published month. A reported **zero** is kept:
it means the plant burned no fuel that month, which is a fact.

The melt is checked against EIA's own published totals rather than trusted.
National net generation reproduces the Electric Power Annual to within a rounding
error — 2010 comes out at 4,125.1 million MWh against EIA's published 4,125 — and
the fuel mix and seasonality are right (2010: coal 1,834, gas 988, nuclear 807,
hydro 260, wind 95 million MWh; a July peak and an April trough).

### 4. The fuel cost is in cents, and the physical units are not one unit

`fuel_cost_per_mmbtu` is published in **cents** per MMBtu. Taken at face value it
overstates every delivered fuel price by 100×. It is divided by 100 here, and the
column carries `usd`.

The physical quantity columns are the opposite problem: EIA reports coal in short
tons, natural gas in thousand cubic feet and oil in barrels, **in the same
column**, with the unit named per row in `fuel_unit`. Those columns therefore
carry no `measurement_unit` and their observations say why, in all three
languages, and point at the MMBtu columns beside them, which are the ones that
can be summed across fuels. Declaring a single unit there would be wrong for most
of the rows.

## Other decisions

**`generator` unions three sheets.** From 2009 EIA splits operable, proposed and
retired generators across three sheets of one workbook; before that operable and
retired share a sheet and proposed generators live in their own file.
`generator_status_group` records which, derived from the sheet for 2009 onward
and from `operational_status_code` before that — using the vocabulary's own
`operational_status` field, so the mapping cannot drift from the code list it
describes. The union is built with `pandas.concat`, which aligns on column name;
the pages carry different column sets and a positional union would shift values
between columns. **Summing `capacity_mw` without filtering
`generator_status_group` adds installed, proposed and retired capacity into one
number**, and the table description says so.

**Dates are month-precision.** EIA-860 never publishes a day for an operating,
retirement or planned date. `operating_date`, `retirement_date`,
`planned_retirement_date` and `planned_operating_date` are built as `YYYY-MM-01`
and every one of them says so in its observations.

**County comes as a name, not a FIPS code.** Both forms publish a county *name*,
so `county_id` is resolved against a committed export of
`br_bd_diretorios_us.county` (`code/us_county_directory.csv`) by normalised name
within state. `state_id` resolves from the postal abbreviation and carries a
`relationships` test; `county_id` does not, because the resolution is not
complete — chiefly the historical Connecticut counties, which EIA still uses and
which the directory replaced with the 2022 planning regions.

**Codes stay as codes.** PUDL relabels `BIT` to `bituminous_coal`; Data Basis
keeps the published code in the column with `covered_by_dictionary = yes` and
puts the label in `dicionario`. A code that is neither valid nor in `code_fixes`
is **kept as published**, not nulled: EIA adds codes faster than any vocabulary
is updated. Only the codes PUDL explicitly lists as meaningless become null.

**calamine, not openpyxl.** The EIA-860 generator workbook is 26,856 rows by 73
columns and openpyxl takes 127 s to parse it against calamine's 27 s, for
byte-identical output (verified by diffing all four 2020 tables cell by cell,
251,464 rows, zero differing cells). Over 26 report years on a pipeline that
rebuilds every year on every run, that is the difference between a two-hour run
and a half-hour one. The `.xls` vintages stay on xlrd.

## Layout

```
models/us_eia_electricity/
├── code/
│   ├── pudl/                  ← vendored from PUDL; generated, never hand-edited
│   ├── architecture/sheet_*.csv   ← source of truth for the schema
│   ├── us_county_directory.csv    ← county name -> FIPS, exported from the directory
│   ├── vendor_pudl.py         refreshes code/pudl/ from a PUDL checkout
│   ├── gen_architecture.py    writes the architecture CSVs (edit column defs here)
│   ├── gen_dbt.py             writes the .sql models and schema.yml from the CSVs
│   ├── gen_columns_json.py    writes the backend column payloads from the CSVs
│   ├── common.py              scratch paths; re-exports the shared transform
│   ├── clean.py               one-shot: clean every year to parquet
│   ├── verify_parquet.py      counts, keys, null shares, magnitudes, coverage
│   ├── upload.py              parquet -> basedosdados-dev staging
│   ├── metadata_spec.py       dataset/table text, in three languages
│   ├── register.py            registers that metadata in a backend, idempotently
│   └── build_auxiliary_files.py
├── schema.yml                 generated
└── us_eia_electricity__*.sql  generated
```

The cleaning transform itself lives in
`pipelines/datasets/us_eia_electricity/utils.py` and is **imported** by
`code/common.py`, never duplicated, so the one-shot bootstrap and the recurring
pipeline cannot drift.

Scratch data goes to `~/Downloads/us_eia_electricity_data/` (override with
`US_EIA_ELECTRICITY_DATA_DIR`) — never in the repo or in Dropbox.

## Recurring pipeline

`pipelines/datasets/us_eia_electricity/flows.py`, daily poll at 09:17 BRT.

**Every run rebuilds every year.** EIA does not extend a report year, it
*republishes* it: a within-year monthly file, then an early release the following
spring, then one or more final revisions, each superseding the last. An
append-only refresh would double every month a revision restates — which is
exactly the failure this dataset's brief warned about. Rebuilding each year's
partition from the newest file EIA serves makes that structurally impossible
rather than inferred from state the pipeline has nowhere to keep, and it keeps
the `dicionario`, which has no year partition, computed over the whole record
rather than over whichever years happened to be refreshed.

The two forms are polled separately because they move on different clocks: 923
gains a month roughly monthly, 860 gains a year once a year. Either being newer
than what is registered is enough to run. A no-op run reads two landing pages and
one ~20 MB ZIP, so the daily poll is cheap.

**Coverage tier.** The house rule paywalls the most recent window of any table
refreshing monthly or more often and leaves lower-frequency tables free, which
splits this dataset exactly along the two forms: `generation_fuel` and
`fuel_receipts_costs` are `PartBdpro` with a 6-month free lag, `plant` and
`generator` are `AllFree`. Both pro tables key the window on `(year, month)`,
which the transform guarantees non-null — a NULL date would fail the Row Access
Policy's `<= free_end` and be paywalled forever.

**Not verifiable before arming:** `apply_row_access_policies` issues real
BigQuery DDL with the worker's rights, and the prod upload needs the worker's
credentials. Both first run on the first armed prod run.
