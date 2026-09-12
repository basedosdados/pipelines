# au_doe_higher_education_finances

Finances of Australian higher education providers, from two Department of
Education collections:

- **Finance Publication** — the audited financial statements every provider
  files, published as four statements per year.
- **Higher Education Research Data Collection (HERDC)** — the research income
  return used to allocate research block grants, published as one time series
  back to 1992.

Licence: CC BY 4.0, © Commonwealth of Australia.

## Tables

| Table | Grain | Coverage |
|---|---|---|
| `income_statement` | provider × year × sector × line | 2008–2024 |
| `balance_sheet` | provider × year × line | 2008–2024 |
| `equity_movement` | provider × year × line | 2009–2024 |
| `cash_flow` | provider × year × line | 2008–2024 |
| `research_income` | provider × year × category × sub-category | 1992–2024 |
| `line_item` | statement × line item | reference |
| `dicionario` | — | reference |

**Two years are missing from the statements: 2010 and 2012.** Their pages are
archived and both name a spreadsheet, but the Wayback Machine never captured
the file — a CDX query for the 2010 release returns the publication's `.docx`
and `.pdf` and no `.xls`, and every capture of the spreadsheet URL returns 404.
Those two years' numbers survive only inside the publication documents, which
this pipeline does not parse. The 2008 release genuinely ships no equity
statement, which is why `equity_movement` starts in 2009.

Providers are keyed on `hep_code` and described in
`br_bd_diretorios_au.higher_education_provider`, added by this onboarding.

## Things that will bite you

**Money is in dollars, everywhere.** The Finance Publication prints its tables
in thousands (`$'000`); the cleaner multiplies by 1,000 so the statements and
`research_income` share one scale. To reconcile against a published table,
divide by 1,000.

**Line item labels drift.** The department relabels statement lines across the
series — 2013's `Income (excl Def Super)` is 2024's `Total Revenues from
Continuing Operations (including Deferred Superannuation)`. A panel built on one
label will silently truncate. Check `line_item` first; it records the years each
label appears.

**A null research income amount is not a zero.** HERDC retires and adds
sub-categories, and prints a blank where one was not in use that year. Those
rows are kept with a null `amount` so the distinction survives.

**Aggregates are dropped.** The source ships state subtotal columns (up to 2021)
and an all-institutions total. Both are recomputable by summing providers, so
neither is stored.

**The sector split is income-statement only.** `institution_type` is `total`
everywhere except the income statement, where dual-sector providers also report
`higher_education` and `vocational_education`.

## Source retrieval

`education.gov.au` times out for plain library user agents but serves fine to a
browser user agent — every request in `code/download.py` carries one.

The live site publishes 2018 onwards. Earlier years were delisted: their pages
404 and their download endpoints return 403, so they come from the Wayback
Machine. Two things make an archived year look absent when it is not:

- **The CDX endpoint throttles a tight loop** and answers with an empty body,
  which is indistinguishable from "never archived". `code/salvage_missing.py`
  walks the remaining years slowly and reports what each attempt actually saw.
- **Releases up to 2016 are legacy `.xls`**, not `.xlsx`. Validating a download
  by ZIP magic alone throws every one of them away. `clean.py` sniffs the magic
  bytes and reads legacy workbooks with `xlrd`; the sheet layout is identical
  across the two formats.

## Statement labels: there are three vocabularies, not one

The workbooks carry two label columns — the reporting cube's internal member
name (`DEEWR Research Grants`, `State Govt Total`) and the published label
(`Education Research Grants`, `State and Local Government Financial
Assistance`). `line_item` is the **published** label, always the last label
column; `line_item_internal` is the cube name. The 2023 release ships a single
label column and no cube name, so reading column 0 as the label mixes the two
vocabularies and reading column 1 as a second label turns 2023's first provider
value into a label.

The 2022–24 CSV export uses a **third** wording again (its `Income Tax` is the
workbook's `Income Tax Expense`), which is why `validate.py` checks values
label-independently as well as by label.

## Code

```
code/
  download.py          fetch both sources (live + Wayback)
  salvage_missing.py   retry the archived years the main pass could not reach
  providers.py         provider label -> HEP code, aliases and aggregates
  clean.py             parse to partitioned parquet
  validate.py          cross-check against the department's own CSV export
  inspect_layout.py    probe workbook layout (diagnostics)
  gen_architecture.py  write the architecture CSVs
  gen_dbt.py           write the dbt models and schema.yml
  upload.py            upload to BigQuery dev staging
  architecture/        column definitions — the source of truth
```

Run order: `download.py` → `clean.py` → `validate.py` → `upload.py`.
Regenerate the schema with `gen_architecture.py` then `gen_dbt.py` after any
column change.

Raw downloads and cleaned parquet live in
`~/Downloads/au_doe_higher_education_finances_data/` (override with
`AU_DOE_HEF_DATA`), never in the repo.
