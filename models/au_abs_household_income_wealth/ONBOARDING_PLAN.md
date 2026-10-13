# au_abs_household_income_wealth — onboarding notes

**Org:** `abs` · **Licence:** CC BY 4.0 · **GCP dataset id / slug:** `au_abs_household_income_wealth`

ABS Household Income and Wealth, Australia (former catalogue 6523.0): the
published summary tables of the Survey of Income and Housing. The survey
microdata are restricted to the ABS DataLab and TableBuilder and are **not**
part of this dataset; the dataset description says so.

## Tables

| Table | Grain | Rows | Notes |
|---|---|---|---|
| `household_estimate` | year × geography × source table × breakdown × measure | 23,728 | long fact from the 16 data cubes, partitioned by `year`, clustered on `breakdown_type` |
| `experimental_wealth_estimate` | year × source table × breakdown × measure | 1,470 | ABS working paper 1351.0, appendix 14.2, 1994–2000 |
| `dictionary` | table × column × key | 32 | decodes `breakdown_type` and `estimate_flag` |

**Coverage.** `household_estimate` spans FY 2007-08 → 2019-20 (`year` = the
calendar year the financial year ends in, so 2008 → 2020, biennial). The
headline income distributions run the full series, wealth from 2009-10; every
other breakdown is a 2019-20 cross-section. `experimental_wealth_estimate`
spans 1994 → 2000, annual and closed.

## Why one long table rather than a table per cube

The 16 cubes are presentation spreadsheets holding 77 tables between them, and
their layouts differ per sheet — some put survey years on the columns, some put
the population breakdown there, some put both. A table per cube would carry
that inconsistency into BigQuery. One long fact table keyed on
(breakdown type, breakdown value, measure) makes a series comparable across
cubes, and `source_table_id` keeps every row traceable back to its ABS cell.

`measure` and `breakdown_value` keep the ABS labels verbatim (levels joined
with `>`), so nothing is renamed away from the source. `breakdown_type` and
`estimate_flag` are coded and decoded in `dictionary`.

## The RSE and margin of error are kept

ABS publishes a relative standard error, or for a proportion a 95% margin of
error, beside each estimate, and suppresses cells it will not publish. Both
error measures are carried as their own columns, and a suppressed cell keeps
its ABS marker in `estimate_flag` (`np`, `na`, `..`) with a NULL `estimate` —
never a zero.

## Parser

`code/cubes.py` reduces a cube sheet to one record per published cell. Three
things about the ABS layout are worth knowing before touching it, each of which
silently corrupts the output if missed:

1. **Row nesting is written two different ways** and one cube mixes them: a
   real cell indent (tables 1.1, 10.x) and three leading spaces inside the
   string (tables 1.2, 13.x). Reading only one flattens half the cubes and
   collapses "Mean income per week / Lowest quintile" onto "Income share /
   Lowest quintile".
2. **Which quantity a cell holds is marked in three different places** — a unit
   in column B, a header row spanning a column group, or an ALL-CAPS banner in
   column A — and a single cube uses all three.
3. **Estimate and error panels do not always carry the same labels.** They are
   matched on the longest row-path suffix that identifies one estimate in that
   column, which absorbs both ABS's rewording and its dropped outer groups.

`code/semantics.py` declares, per source table, which axis holds the
population breakdown and which holds the measure. That cannot be inferred, so
`clean.py` refuses to run against a table it has no entry for.

Verification: 47,083 data cells read, 47,083 emitted (no loss); 15,966 of
15,984 RSE cells and 7,345 of 7,371 margin-of-error cells matched to their
estimate; the remaining 44 are in seven sheets where an error panel has rows
the estimate panel does not. Table 1.1 and table 10.2 were checked value by
value against the workbook.

## Scope: the 2019-20 release only

**ABS renumbers its cubes between releases.** The 2017-18 release inserts an
"INCOME, GOVERNMENT BENEFITS AND TAXES" sheet into cubes 1 and 5 to 13, which
shifts tables 13.5 through 13.12 by one — so 13.5 is equivalised disposable
income for a state in the 2019-20 release and income, benefits and taxes in the
2017-18 one. A table id does not identify the same table across releases, and
running the 2019-20 axis map against 2017-18 would mislabel eight tables'
geography level without failing. `semantics.AUDITED_RELEASE` pins the release
the map was read from and `clean.py` refuses any other, so adding a release is
a deliberate act of auditing its own table map.

There is no 2021-22 release. The 2019-20 release, published 28 April 2022, is
the latest under this title; the landing page lists 2017-18 and earlier as
previous releases.

## Working paper 1351.0

The 1994–2000 wealth-by-age series is not a data cube. It is a PDF-only ABS
working paper, and its body shows the series only as charts — even the item
labelled "Table 4.2.4" is a bar chart. The numbers exist in appendix 14.2, as
30 tables in the PDF's text layer, which `code/paper1351.py` reads after
`pdftotext -layout`. The appendix's column headers wrap over up to five lines,
so they are declared in that module and the parser asserts each row carries
exactly as many values as labels.

These estimates are modelled from the national accounts household balance
sheet, benchmarked to the Survey of Income and Housing Costs and the Household
Expenditure Survey, were published as experimental and carry no standard
errors. They are **not comparable** with the survey estimates, so they are held
in their own table and the table description says they must not be joined to
`household_estimate` as one series.

## Recurring pipeline

None. The Survey of Income and Housing is biennial at best, and no release has
followed 2019-20 in over four years. The next release, whenever it comes, will
renumber the cubes, so it needs a fresh axis audit rather than a scheduled
refresh. Re-onboard with `code/download.py` → `code/clean.py` → `code/upload.py`.

## Files

```
models/au_abs_household_income_wealth/
  code/download.py     cubes zip + working paper PDF -> input/
  code/cubes.py        structural parser: a cube sheet -> one record per cell
  code/semantics.py    per-table axis roles, breakdown vocabulary, labels
  code/paper1351.py    appendix 14.2 of working paper 1351.0
  code/clean.py        input/ -> output/ (all-string partitioned parquet)
  code/upload.py       output/ -> basedosdados-dev staging
  code/architecture/   household_estimate.csv, experimental_wealth_estimate.csv,
                       dictionary.csv (source of truth for columns)
  au_abs_household_income_wealth__household_estimate.sql
  au_abs_household_income_wealth__experimental_wealth_estimate.sql
  au_abs_household_income_wealth__dictionary.sql
  schema.yml
```

Scratch data lives under `~/Downloads/au_abs_household_income_wealth_data/` and
is deleted once the onboarding is verified.
