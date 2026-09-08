# us_nih_reporter — design notes

NIH **RePORTER ExPORTER**: every research project funded by the National
Institutes of Health and six sibling agencies since fiscal year 1985, with the
principal investigators, the awardee institution, the funding amount, the study
section, the activity code, and the publications, patents and clinical studies
that cite the project as support. Source, cleaning code and dbt models all live
under this directory and `pipelines/datasets/us_nih_reporter/`.

## Source

<https://reporter.nih.gov/exporter> — six file families reached through
`/exporter/<family>/download[/<year>]`:

| family | clean table | year key | files |
|---|---|---|---:|
| `projects` | `project` | fiscal, FY1985-FY2025 | 41 |
| `abstracts` | `project_abstract` | fiscal, FY1985-FY2025 | 41 |
| `publications` | `publication` | calendar, 1980-2025 | 46 |
| `linktables` | `publication_link` | calendar, 1980-2025 | 46 |
| `patents` | `patent_link` | none — one all-years file | 1 |
| `clinicalstudies` | `clinical_study_link` | none — one all-years file | 1 |

Plus one accessory file, `RePORTER_PRJFUNDING_C_FY1985_FY1999.zip`, reached
through the generic document service
(`/services/exporter/DownloadFromDocService?DocType=EXPFND&KeyId=1985`) rather
than the family route. It carries the costs and DUNS numbers missing from the
FY1985-FY1999 project files.

3.6 GB of archives in total. The ExPORTER listing page is a JavaScript
application with no server-rendered file list and no documented listing
endpoint, so the file inventory is established by probing the download route
rather than scraped.

Documentation: the [ExPORTER data
dictionary](https://report.nih.gov/exporter-data-dictionary) and the ExPORTER
section of the [RePORT FAQs](https://report.nih.gov/faqs), both fetchable and
both bundled as auxiliary files. The [NIH activity code
register](https://grants.nih.gov/grants/funding/ac_search_results.htm) is not —
see trap 7.

Licence: work of the US federal government, public domain under 17 U.S.C. §105.
Registered as `cc0`, the house convention for US federal works. RePORTER's own
footer reserves the right to block automated queries that degrade service for
other users, which is a rate-limit condition, not a licence restriction.

## Relationship to us_treasury_usaspending

Complementary, and the descriptions say so. USAspending is the *budgetary*
record — the federal award transaction, its recipient and its amount. RePORTER
is the *scientific* record of the same money: which study section reviewed the
application, which activity code funds it, who the principal investigators are,
what the abstract says, and which papers, patents and trials came out of it.
Neither substitutes for the other.

No linkage between the two is constructed here beyond what NIH itself publishes.

## The seven traps

Each was measured over the whole corpus, not assumed. `verify_parquet.py`
re-checks the ones that can regress.

### 1. The project file's header changes four times, and its column order with it

Four distinct headers across FY1985-FY2025:

| variant | fiscal years | columns | notable |
|---|---|---:|---|
| 0 | 1985-1989, 1991 | 42 | `SUBPROJECT_ID` is column **14** |
| 1 | 1990, 1992-2005 | 42 | `SUBPROJECT_ID` is column **40** |
| 2 | 2006-2024 | 46 | adds `FUNDING_MECHANISM`, `ORG_IPF_CODE`, `DIRECT_COST_AMT`, `INDIRECT_COST_AMT`; renames `FOA_NUMBER` to `OPPORTUNITY NUMBER` (with a space); starts quoting every field |
| 3 | 2025 | 46 | renames `CFDA_CODE` to `ASSISTANCE_LISTING_NUMBER` |

Reading by position would transpose `SUBPROJECT_ID` against `FUNDING_ICs` for
six fiscal years. Every file is read by **header name**, and
`constants.HEADER_RENAME` folds the two renames onto one name each.

### 2. The component columns are not a decomposition of the project number

`CORE_PROJECT_NUM` is a substring of `FULL_PROJECT_NUM` on **all 2,951,523**
project rows. It disagrees with `ACTIVITY + ADMINISTERING_IC + SERIAL_NUMBER` on
**91,147 rows (3.09%)**, and a full parse of `FULL_PROJECT_NUM` against the
source's own component columns disagrees on 63,958 rows (2.17%). The cause is
visible in the examples:

```
FY2004  full 5R01CA089566-04   activity column says U01
FY2007  full 1U38HK000002-01   administering_ic column says CD
```

The administering institute moves when an award is transferred; the number keeps
the letters it was minted with. Both are correct, for different questions.

So both are shipped and **neither is derived from the other**:
`core_project_num` is the join key to `publication_link`, `patent_link` and
`clinical_study_link`; the component columns describe the award as it now
stands. Re-parsing the string into a second set of component columns was
considered and rejected — it would ship six columns that disagree with the
source's own on 2-3% of rows without saying which is right.

### 3. Dates arrive in three formats

| column | `YYYY-MM-DD` | `M/D/YYYY` | with a time |
|---|---:|---:|---:|
| `PROJECT_START` | 1,442,129 | 1,125,502 | 0 |
| `BUDGET_START` | 1,550,310 | 1,075,305 | 0 |
| `AWARD_NOTICE_DATE` | 1,487,012 | 0 | 817,361 |

`M/D/YYYY` is the dominant shape through FY1985-FY2005. A plain
`safe_cast(... as date)` returns NULL for it, which would empty
`project_start` and `budget_start` for two decades. `norm_date` normalises all
three to `YYYY-MM-DD` before the parquet is written.

### 4. Costs are absent from FY1985-FY1999 and published separately

The main project files for those fiscal years carry no `TOTAL_COST` at all. NIH
publishes `TOTAL_COST`, `TOTAL_COST_SUB_PROJECT`, `FUNDING_ICS` and `ORG_DUNS`
for them in an accessory file and documents the join on `APPLICATION_ID`. The
transform merges it, filling only where the main file is blank —
`load_funding_supplement` reads 840,226 rows.

### 5. `ORG_STATE` is not a US state field

70 distinct values. Eleven are Canadian provinces (`ON` 2,728, `PQ` 1,356, `BC`
1,058, `AB` 431, `QC` 286, `MB` 159, `NS` 62, `SK` 20, `NL` 6, `NB` 2, `PE` 2)
and eight are territories or freely associated states (`PR`, `GU`, `VI`, `AS`,
`MP`, `PW`, `MH`, `FM`). `NS` and `NL` collide with nothing US, but a link to a
US state directory would still be wrong for those 6,110 rows, so the column
carries **no `directory_column`** and says why in its observations.

`ORG_FIPS` has the matching problem in reverse: it is the FIPS 10-4 country
code, not ISO — the United Kingdom is `UK`, Switzerland is `SZ`.

### 6. One publication file carries a UTF-8 byte order mark

`PUB_2021.zip` — and only that one, out of 191 archive members. Decoded as plain
utf-8, the mark stays glued to the first header name and stops `csv`
recognising the field as quoted, so the header reads `﻿"AFFILIATION"` and
the `affiliation` column comes out **empty for all 113,958 rows of 2021**. Files
are decoded `utf-8-sig` first, and header names are stripped of any surviving
quote. Verified after the fix: 111,542 of 113,958 rows carry an affiliation.

### 7. The activity code register is browser-only

`grants.nih.gov/grants/funding/ac_search_results.htm` answers **403 to every
scripted request** — plain `requests`, a browser user agent, and a
TLS-impersonating `curl_cffi` client alike — while serving the same page to a
browser session. The 258-code register is therefore **committed** at
`code/reference/activity_codes.csv` rather than fetched at run time, and it is
what turns `activity` from a three-character code into a programme name in the
dicionario. 372 activity codes appear in the data and **233 of them get a
label**; the register covers grants and cooperative agreements and leaves
contract (`N…`), intramural (`Z…`) and non-NIH agency codes unlabelled. That is
why `activity` is in `DICT_COLUMNS` but not in `DICT_TEST_COLUMNS` — the dbt
`custom_dictionary_coverage` test would fail on data that is correct.

## What the build produced

`verify_parquet.py` reads blank counts from the parquet row-group statistics
rather than the data, and reduces each key to a 64-bit hash in a numpy array
rather than a Python set, so the whole audit peaks at ~590 MB against a 4.7 GB
corpus.

| table | rows | files | key | duplicates |
|---|---:|---:|---|---:|
| `project` | 2,951,523 | 41 | `year, application_id` | 0 |
| `project_abstract` | 2,599,720 | 41 | `year, application_id` | 0 |
| `publication` | 3,218,880 | 46 | `year, pmid` | 0 |
| `publication_link` | 7,582,090 | 46 | `year, pmid, core_project_num` | 0 |
| `patent_link` | 92,936 | 1 | `patent_id, core_project_num, patent_org_name` | 0 |
| `clinical_study_link` | 39,560 | 1 | `nct_id, core_project_num` | 0 |
| `dicionario` | 497 | 1 | `id_tabela, nome_coluna, chave` | 0 |

**`patent_link`'s key includes the owner, and that is not cosmetic.**
`(patent_id, core_project_num)` repeats on 36 rows — every one of them the same
patent title under two different owners, such as patent 9370529 on P50AT004155
credited to both Iowa State University and the University of Iowa. The same
patent, supported by the same project, reported by two institutions. The triple
is unique on all 92,936 rows, so it is the key; collapsing the pair would have
silently discarded a real co-assignment.

### Join integrity, measured

| join | matched | note |
|---|---|---|
| `publication_link` → `project` | 7,239,514 / 7,582,090 (95.48%) | see below |
| `patent_link` → `project` | 92,928 / 92,936 (99.99%) | |
| `clinical_study_link` → `project` | 39,381 / 39,560 (99.55%) | |
| `project_abstract` → `project` | 2,598,777 / 2,599,720 (99.96%) | 943 unmatched |

The 4.5% of publication links with no project is **the project file's start
date, not a defect**. The unmatched share falls from 58% in the 1980 link file
to about 1% from 2020 on, and the unmatched project numbers use activity codes
that went out of use (R23, K04, T01) and the old institute letters (`AM`, the
arthritis and metabolic institute now `DK`). Publications from 1980-1984 cite
grants awarded before FY1985, which is where ExPORTER's project file begins.

The 943 unmatched abstract rows are an inconsistency between the source's own
two files for the same fiscal year. They are kept as published.

**No `ignore_values` anywhere in `schema.yml`.** Every column of every table
clears the 0.05 non-null floor in the newest partition by a wide margin — the
list of sparse columns this design started with was a guess, and the
measurement removed it. The `not_null_proportion_multiple_columns` and
`custom_dictionary_coverage` tests are scoped to `__most_recent_year_en__` on
the four partitioned tables, because the test scans every column and the whole
`project_abstract` table is 8 GB of text.

## Also worth knowing

**The download endpoint intermittently 404s a file that exists.** One of 176
downloads failed that way and succeeded on the next attempt, so `download_file`
retries a 404 rather than reading it as absence.

**`APPLICATION_ID` is unique within a fiscal year without exception**, and
repeats exactly once across the whole corpus (`11169374`). The key is therefore
`(year, application_id)`, not `application_id` alone.

**`PMID` is not unique across the corpus either.** 22,930 PMIDs appear in more
than one calendar-year publication file, so `publication`'s key is
`(year, pmid)`.

**`APPLICATION_TYPE` has two undocumented values.** The data dictionary defines
1, 2, 3, 4, 5, 7 and 9; the data also contains 6 (4,885 rows) and 8 (2,213
rows). The dicionario registers them as undefined rather than dropping them.

**Fiscal versus calendar year.** `project` and `project_abstract` are keyed by
the federal fiscal year (1 October to 30 September, named for the year it ends).
`publication` and `publication_link` are keyed by the calendar year of the
release file. Every year column's description and observations say which.
`publication.publication_year` equals `publication.year` on all 3,218,880 rows,
so the distinction costs nothing there — but it is real for `project`.

**Patents are NIH-only and incomplete by the source's own account.** RePORTER
lists issued patents only, not applications, and reports them for NIH projects
only — not for the ACF, AHRQ, CDC, FDA, HRSA or VA projects that also appear in
`project`. NIH also notes that recipients often stop reporting to iEdison once
support ends.

## PI names

`pi_names` and `program_officer_name` are the names of principal investigators
and programme officers as published in the grant record — public officials of a
public award, and already the primary way RePORTER itself is searched. They are
carried as published. No additional linkage across datasets is constructed
beyond what NIH itself publishes, and `has_sensitive_data` is `no` on every
column.

## Pipeline

Two flows, because one cadence cannot serve both halves of the source:

* **`us_nih_reporter`** — the annual corpus. NIH creates the consolidated
  project and abstract files at the close of each fiscal year, restates the
  three prior fiscal years at the same time, and updates the publication and
  link files then too. FY2008-FY2023 all carry the same publication date,
  2025-06-16: one sweep that rewrote sixteen fiscal years at once. Polled three
  times a month; each run rebuilds everything.
* **`us_nih_reporter_links`** — patents and clinical studies, rewritten roughly
  weekly and weighing 16 MB together. Polled weekly.

The poll reads `Last-Modified` from the document service — two header requests
per file, no download — and compares the newest against `Table.Update.latest`
with `compare_against="table_update"`, because the signal is a publication
timestamp rather than a coverage date. Fiscal-year coverage does not move when
NIH restates FY2019.

Every table is `AllFree`: the BD Pro rolling window covers tables refreshed
monthly or more often, the annual tables are not, and the two link tables have
no date column for a window to slide along.

## Auxiliary files — built, not uploaded

`build_auxiliary_files.py` writes one bundle per table under
`<scratch>/auxiliary_files/<table>/auxiliary_files.zip`: the ExPORTER data
dictionary, the RePORT FAQ page, a README with the citation, per-file
provenance and download date, and the notes a reader needs for that table.
`project`'s bundle additionally carries the activity code register.

**They are not in GCS yet.** The dev service account this onboarding runs under
(`chave-subidores-de-dados@basedosdados-dev`) gets 403 writing to
`gs://basedosdados`, so the upload needs a credential with prod object-write
rights. Run `build_auxiliary_files.py --upload` under one.

`auxiliary_files_url` is registered on all seven tables regardless, at the
documented path. Fetched anonymously today it returns **HTTP 400
`UserProjectMissing`** — both buckets are requester-pays, which is why every
production `auxiliaryFilesUrl` is currently dead for a site visitor, not
something specific to this dataset. Verified against
`us_noaa_storm_events/event`, which returns the same.

## Backend IDs (staging)

Recorded so a re-run does not create duplicates —
`create_update_observation_level`, `create_update_cloud_table`,
`create_update_coverage` and `create_update_update` all create a second record
when called without an `id`.

| what | id |
|---|---|
| organization `national_institutes_of_health_nih` | `199afe96-0f70-4c5d-be43-d8e50fcafed2` |
| licence `cc0` | `7fb71004-2abe-4fc8-a258-e2aac27c71d9` |
| availability `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |
| status `under_review` / `published` | `47208305-…` / `e16221de-…` |
| area `us` | `61a2c232-c649-4b41-a5a3-1467b7393e11` |
| account | `57` |
| entity `year` / `project` / `article` / `patent` / `other` | `e1bf146e-…` / `c5b8b0a3-…` / `90a4d427-…` / `d2084929-…` / `1b3a7364-…` |
| dataset `nih_reporter` | `b49928d1-d6c1-4a38-a590-f909a6de514f` |
| raw source — annual bulk files | `c95b4cb5-083a-4e9b-8421-af15d1da76af` |
| raw source — patents and clinical studies | `45f4e00d-1789-49c8-970f-06c82c733add` |
| raw source — RePORTER API | `6f31581a-d53f-4392-b8e0-a00d95d813f9` |
| table `project` | `18a9d8c2-5b50-4ee0-bb8c-14a03f8466cc` |
| table `project_abstract` | `fb9d6653-7d41-499a-b550-b016f44e83fa` |
| table `publication` | `60276dd4-9e5f-41bd-84fa-8806db851ff0` |
| table `publication_link` | `b9d34a49-bee8-48f6-bdfe-19919df845fa` |
| table `patent_link` | `f8e76658-772f-4293-84e8-ad42ef25d715` |
| table `clinical_study_link` | `d695a4e1-cd9f-4aa2-8494-6b229ee3233c` |
| table `dicionario` | `145237e4-54af-4032-99df-3fc471c8c03d` |

`patent_link` and `clinical_study_link` carry a `us` coverage with **no**
datetime range: the source ships them as one snapshot covering every fiscal
year with no date column, so a range would be a claim the data does not make.
The pipeline's `NonHistorical()` spec derives one from the BigQuery table's
last-modified on the first armed prod run.

## Backend IDs (prod)

Registered `status = under_review`, so the dataset is hidden from the public
frontend until the PR merges, table-approve materialises
`basedosdados.us_nih_reporter.*`, and the tables are verified. Publishing is
step 13, a separate action.

| what | id |
|---|---|
| dataset `nih_reporter` | `f17727fb-4553-40e1-a495-676805a878c1` |
| raw source — annual bulk files | `bea1b1ad-f846-4327-8aca-140d89c26472` |
| raw source — patents and clinical studies | `41ff2d80-c961-4651-b7d8-261be8435142` |
| raw source — RePORTER API | `04ee1972-2720-4610-8a4c-ab9087ad1782` |
| table `project` | `05237e77-d923-4607-a2d0-a024e3da7944` |
| table `project_abstract` | `e4e395fc-43db-4405-8c6a-3b26fda577e0` |
| table `publication` | `a71f40fb-ae16-431e-b30c-9a19ceff8050` |
| table `publication_link` | `86e8ff88-7091-473f-a913-d7de3a099513` |
| table `patent_link` | `6c2478f7-f977-4fe1-a283-47de9d9689fe` |
| table `clinical_study_link` | `1de56866-0f01-40bb-821c-c04951e3dfe6` |
| table `dicionario` | `3c9211c6-1aeb-425b-b6e4-7f0ad0219b20` |
| account | `4` |

**Reference ids are not interchangeable between the two environments, and the
tag slugs are not either.** The `project` entity is `c5b8b0a3-…` on staging and
`53374e81-…` on prod. Staging's tag vocabulary is Portuguese and prod's is
English — `pesquisa` there is `research` here, on the same UUID — so
`register_metadata.py` lists both spellings per tag and takes whichever the
environment has, rather than resolving one slug and failing on the other.

## The `clinical_study` entity

Created for this dataset, under the `health` category alongside `aih`,
`health_care_provider` and `notification`:
`e98f8fd1-6928-4c39-a06c-f8974bc3f6f8` on staging,
`991ca75f-4a62-4940-a2f2-36195318c78b` on prod. It is shared reference data, so
any dataset with a clinical-trial grain should reuse it rather than fall back to
`other`.

`discover_ids` cannot read entity categories — it queries `allEntityCategory`
and the schema calls the field `allEntitycategory` — so the category is resolved
through `_gql` instead. That is the reason earlier onboardings recorded new
entities as impossible to create.

The staging observation level was rewritten in place rather than replaced: there
is no MCP tool to delete an observation level, and the column's FK points at its
id, so reusing the record keeps `nct_id` linked and leaves no orphan behind.

**The three raw sources exist so each table has exactly one.**
`client._raw_source_id` raises when a table has two or more, and both the poll
and the commit tasks go through it — a table linked to two sources cannot run a
recurring pipeline at all. The annual families point at one, patents and
clinical studies at another, and the RePORTER API is registered at dataset
level with no table link, because it is documented as a source but is not where
these tables come from.
