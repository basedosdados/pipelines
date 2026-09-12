# us_nsf_ncses

Two surveys run by the U.S. National Center for Science and Engineering
Statistics (NCSES), the federal statistical agency inside the National Science
Foundation.

| Survey | Grain | Coverage | Source |
|---|---|---|---|
| Higher Education Research and Development (HERD) | institution x fiscal year x questionnaire cell | FY1972–FY2024 | [public use data files](https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development) |
| Survey of Earned Doctorates (SED) | published table x cell | academic years 1958–2024, 2024 cycle | [published data tables](https://ncses.nsf.gov/surveys/earned-doctorates) |

## Tables

| Table | Rows | What it holds |
|---|---:|---|
| `herd_institution` | 36,125 | one row per institution and fiscal year: identity, location, HBCU / medical school / control flags |
| `herd_expenditure` | 5,379,896 | every monetary questionnaire cell, in current dollars |
| `herd_personnel` | 29,586 | headcount and full-time equivalents of R&D personnel |
| `herd_survey_item` | 67,589 | the questionnaire items that are neither expenditures nor personnel |
| `sed_estimate` | 61,179 | one row per cell of the 96 published SED data tables |
| `sed_data_table` | 96 | catalogue of those tables: id, title, group, unit statement |
| `dicionario` | 65 | value to label for every coded column |

## One dataset, not two

Production already held an empty shell for the SED alone
(`survey_of_earned_doctorates_sed`, id `60aaf560-d82a-42b9-bf86-155d18b8c5bf`).
This onboarding fills that record by id and broadens it to the agency rather than
creating a second dataset beside it. Three reasons:

1. **One GCP dataset.** Both surveys' tables live in `us_nsf_ncses`, and the
   `<country>_<org>_<slug>` convention derives that name from the backend slug.
   Splitting the backend in two while the cloud tables stay in one BigQuery
   dataset makes the slug stop describing where the data is.
2. **The shell was empty** (`tables: {}`), so broadening it orphans nothing and
   leaves no duplicate for a reader to choose between.
3. **Precedent in this repo.** `au_abs_cpi` was generalised into
   `au_abs_prices_inflation` (PR #2033) for exactly this shape: a narrow record
   grown into the agency's topic rather than given a sibling.

The two surveys also share one agency, one licence, one release cadence and one
documentation site, and keeping them together is what makes the chain legible on
a single page: `us_nih_reporter` is the federal grant money coming in,
`herd_expenditure` is what institutions spend, `sed_estimate` is the researchers
the system produces.

The slug moves from `survey_of_earned_doctorates_sed` to `ncses`, so the public
URL changes.

## Joining to the rest of the repo

`herd_institution.unitid` and the `unitid` on each HERD fact table carry the
IPEDS UNITID, the same key as `us_ed_ipeds` and `us_ed_college_scorecard`.
`us_ed_ipeds` types it `INT64`, so a join needs a cast:

```sql
select h.year, i.institution_name, h.expenditure / 1e9 as total_rd_usd_billion
from `basedosdados.us_nsf_ncses.herd_expenditure` h
join `basedosdados.us_ed_ipeds.hd` i
  on safe_cast(h.unitid as int64) = i.unitid and h.year = i.year
where h.year = 2023 and h.question = 'Source' and h.row_label = 'Total'
order by h.expenditure desc
```

Run on dev, that returns UCSF at $2.05 billion, Penn at $1.95 billion and
Michigan at $1.93 billion for FY2023 — the same order and the same magnitudes as
NCSES's own published ranking.

The source carries the UNITID only from FY2010. For FY1972–FY2009 it is carried
back from the same NCSES institution id observed in FY2010 or later; an
institution that left the survey before FY2010 has none, which is 9 per cent of
`herd_institution` rows.

SED institution names are published in an abbreviated form ("U. Michigan, Ann
Arbor") with no identifier, so SED does not join to IPEDS.

## Reconciliation

The cleaning code was checked against NCSES's own published totals rather than
against itself:

| Check | Computed here | Published |
|---|---|---|
| FY2024 total higher education R&D | $117.7 billion | $117.7 billion |
| FY2010 total | $61.3 billion | $61.2 billion |
| FY1990 total | $16.3 billion | $16.3 billion |
| 2024 research doctorates (SED table 1-1) | 58,131 | 58,131 |
| FY2023 top institution, joined through IPEDS | UCSF, $2.05 billion | UCSF, $2.05 billion |

## Things that will bite

* **Expenditures are published in thousands of dollars.** `expenditure` and
  `amount` are multiplied by a thousand and hold dollars, so the source
  precision is the thousand. Current dollars, not deflated.
* **Two survey eras.** FY1972–FY2009 is the Survey of R&D Expenditures at
  Universities and Colleges; FY2010 onwards is HERD. `hbcu_indicator`,
  `high_hispanic_enrollment_indicator` and `highest_degree_code` all change code
  sets at that boundary, which is why every `dicionario` entry carries its own
  temporal coverage.
* **No personnel data for FY2020 and FY2021.** The old personnel item ran to
  FY2019, the new one starts in FY2022, and the public files carry neither for
  the two years between.
* **SED cycles are vintages.** Each cycle republishes its own history, so filter
  to the largest `reference_year` instead of summing across cycles.
* **`sed_estimate` is keyed on worksheet position.** Table 5-3 indents two
  different sections at the same level, so two distinct rows share a `row_path`;
  `(table_id, row_number, column_number)` is what identifies a cell.
* **`sed_estimate.value` has no single measurement unit.** Counts, percentages,
  current dollars and medians of years share the column; the `unit` column says
  which, per cell.

## Recurring refresh

`pipelines/datasets/us_nsf_ncses/` holds one annual Prefect flow for both
surveys. One flow rather than two: every table's `custom_dictionary_coverage`
test reads `ref('us_nsf_ncses__dicionario')`, and the dictionary is written by
the HERD half, so a SED-only flow would depend on a staging table it does not
own.

The flow reads the newest published HERD fiscal year and SED cycle straight off
the NCSES pages, polls each against the registered coverage, and returns without
downloading anything when neither has moved. When either has, it rebuilds
*everything*: HERD retro-imputes prior years on each release and an SED cycle
republishes its whole series, so appending only the newest partition would leave
stale numbers behind.

`dump_mode="append"` is deliberate. It ends in
`st.upload(..., if_exists="replace")`, replacing each blob by name; `"overwrite"`
calls `tb.delete(mode="all")`, which drops the materialized **production** table
even from a dev-only run.

Schedule: `34 7 5,12,19,26 8,9,10,11,12 *` (America/Sao_Paulo) — weekly through
the August-to-December release window.

**Not yet run.** Local checks pass — the flow imports, `deploy_flows` discovers
it, and the source probes return FY2024 and cycle 2024 — but those cannot reach
the upload, poll or dbt halves. The flow is not done until it has run on the dev
pool with `{"materialize_to_prod": false, "update_metadata": false,
"force_run": true}` and the logs show `dbt run OK` and `dbt test OK` for all
seven tables. That needs the PR to carry the **`deploy-flow`** label.

## Running it

```sh
python models/us_nsf_ncses/code/herd_clean.py          # 66 ZIPs -> parquet
python models/us_nsf_ncses/code/sed_clean.py           # 96 workbooks -> parquet
python models/us_nsf_ncses/code/build_architecture.py  # architecture CSVs
python models/us_nsf_ncses/code/build_dbt_models.py    # dbt SQL from those CSVs
python models/us_nsf_ncses/code/upload.py              # -> basedosdados-dev staging
uv run dbt run  --select models/us_nsf_ncses
uv run dbt test --select models/us_nsf_ncses
~/.venvs/bd-pipelines/bin/python models/us_nsf_ncses/code/metadata.py staging
```

Downloads and parquet go to `~/Downloads/us_nsf_ncses_data/`, never into the
repo. The NCSES server is slow and drops connections part way through a file, so
the downloader resumes and checks the finished size against `Content-Length`; a
plain `curl` returns HTTP 200 on a truncated file.

## Not onboarded

* **SED individual microdata.** Restricted use, obtainable from NCSES only under
  a licence agreement. Out of scope by design.
* **The HERD published data tables** (NSF 26-304). They are aggregations of the
  institution-level public use files that are already here in full.
