# us_ed_nces_ccd — NCES Common Core of Data

The annual universe of United States public schools, school districts and their
students, staff and finances, 1986–2024. This is the K–12 counterpart to
`us_ed_ipeds`.

| Table | Grain | Rows | Coverage |
|---|---|---|---|
| `school` | school × year | 3.79 M | 1986–2024 |
| `school_district` | district × year | 701 K | 1986–2024 |
| `school_enrollment` | school × year × grade × race × sex | 429.2 M | 1986–2024 |
| `staff` | district × year × staff category | 10.2 M | 1986–2024 |
| `district_finance` | district × fiscal year | 500 K | 1989–2020 |
| `dicionario` | code lookups | 657 | — |

## Where the data comes from, and why

The brief named the NCES CCD flat files (`nces.ed.gov/ccd/files.asp`) and the
Census F-33 as sources, with the Urban Institute Education Data Portal as a
harmonization reference. The data here is downloaded from **Urban**, and that
is a deliberate change of source rather than a shortcut.

The raw NCES files do not form a panel. Across the 39 years they change shape
repeatedly: schools are split across three regional files before 1998, the
membership file is wide until 2016-17 and long after, the race standard moves
from five categories to seven in 2008-09, and the modern per-component files
(`ccd_sch_052_*`, `ccd_lea_059_*`) only exist from 2014-15. Reconstructing a
consistent 1986–2024 panel from them is the work Urban has already done and
publishes under ODC-By.

Urban also publishes bulk CSVs — not just the paginated API — which is what
makes the enrollment table tractable at all. The same series pulled through the
JSON API would be on the order of a billion rows across roughly a hundred
thousand paginated requests.

The NCES file catalogue is reachable programmatically, which is how the file
inventory above was verified:

```bash
curl -s "https://nces.ed.gov/ccd/datatables/api/File/0/0/0/0/0/0"
```

## Conventions worth knowing before querying

**`year` is the fall of the school year.** `year = 2020` is school year
2020-21. For `district_finance` that is fiscal year 2021 — the F-33 collected
for school year 2020-21.

**Missing values are negative sentinels in the source, and are NULL here.**
Urban writes `-1` (missing / not reported), `-2` (not applicable) and `-3`
(suppressed) into every column. All three are mapped to NULL on load, per
column — with one exception.

**`grade = -1` is prekindergarten, not a missing value.** A blanket
negative-to-NULL rule would silently delete every prekindergarten enrollment
row. `grade` is the only sentinel-exempt column.

**`school_enrollment` contains its own marginal totals.** Code `99` on `grade`,
`race` and `sex` means "total across that dimension", so the table holds both
the cells and their margins. Summing without filtering multiply-counts. The
totals cannot simply be dropped: race and sex detail only begins in 1998, and
1986–1997 consists of totals alone.

```sql
-- school-level total enrollment, no double counting
select year, school_id, enrollment
from `basedosdados.us_ed_nces_ccd.school_enrollment`
where grade = '99' and race = '99' and sex = '99'
```

**`staff` totals sit alongside their components.** `teachers_total`,
`guidance_counselors_total`, `lea_staff_total`, `school_staff_total` and
`staff_total` are reported categories in the same column as the components they
aggregate. Filter, do not sum across all categories.

**Identifiers are re-padded on load.** Urban strips leading zeros from `leaid`
in the enrollment extracts (93,404 rows in 1986 alone), and preserving them
matters for joins. `census_id` and the ZIP columns are kept verbatim for the
same reason — normalising them through a numeric cast turns ZIP `01005` into
`1005`. One source row carries an 11-character `ncessch`
(Ashfield-Plainfield Regional, Massachusetts, 1986); it is corrected explicitly
to `250000301636` in every table so the join holds.

## Known gaps

**The US school and district directories are a single-year snapshot.**
`br_bd_diretorios_us.school` and `.school_district` are built from the 2023-24
CCD directory only, so they describe schools open in that year and nothing
else. `school_id` and `agency_id` here carry the `directory_column` link,
because the semantic relationship is right, but **no dbt `relationships` test
is attached** — a 39-year panel contains every school that has since closed,
and the test would fail on all of them. Making the link enforceable would mean
rebuilding those directory tables as the union across all CCD years, or
versioning them by year the way `br_bd_diretorios_au` versions ASGS.

**`state_id` is not a strict foreign key either.** The CCD extends the state
FIPS list with codes for jurisdictions that are not states — 58 (Department of
Defense schools overseas), 59 (Bureau of Indian Education), 61 and 63
(Department of Defense areas). The directory holds the 60 real FIPS state and
territory codes and nothing else, so the extra codes are covered by the
dictionary instead.

**Three measurement units the dataset needs do not exist in the backend
vocabulary**: full-time equivalent, decimal degree, and a count of schools.
Those columns carry no unit and state it in the description instead. There is
no MCP tool to create a measurement unit.

## Layout

```
code/
  schema.py            column specs for all six tables — the source of truth
  utils.py             the cleaning transform, shared with the Prefect pipeline
  clean_data.py        one-shot entrypoint: download → partitioned Parquet
  build_artifacts.py   → architecture/*.csv, dicionario_values.csv, columns.json
  build_dbt_files.py   → ../*.sql and ../schema.yml
  upload.py            → BigQuery dev
```

Everything downstream is generated from `schema.py`, so the architecture CSVs,
the dbt models, `schema.yml` and the backend column payload cannot drift apart.
After editing it, re-run both builders.

```bash
uv run --no-project python models/us_ed_nces_ccd/code/build_artifacts.py
uv run --no-project python models/us_ed_nces_ccd/code/build_dbt_files.py
uv run pre-commit run --files models/us_ed_nces_ccd/schema.yml \
    models/us_ed_nces_ccd/*.sql models/us_ed_nces_ccd/code/architecture/*.csv
```

The pre-commit pass is not optional: `sqlfmt`, `yamlfix` and the line-ending
hook rewrite the generated SQL, YAML and CSV, so a commit straight after the
builders always fails the first time.

Scratch data lives under `~/Downloads/us_ed_nces_ccd_data/` (override with
`CCD_DATA_DIR`) and is never committed.

## Recurring pipeline

`pipelines/datasets/us_ed_nces_ccd/` appends one school year per release. The
portal publishes roughly 18–24 months after the school year ends, so the flow
polls a few days a month from September to December and returns immediately
when nothing new has landed. It imports the transform from `code/utils.py`
rather than reimplementing it. `district_finance` is not on the schedule: the
F-33 stops at 2020 on the portal and moves on its own cadence.
