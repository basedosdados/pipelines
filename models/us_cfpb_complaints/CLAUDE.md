# us_cfpb_complaints — CFPB Consumer Complaint Database

Consumer complaints submitted to the US Consumer Financial Protection Bureau about
consumer financial products, one row per complaint, from December 2011 onwards.
Pairs with `us_cfpb_hmda` under the same organization (`cfpb`).

## Tables

| Table | Rows | Grain | Partition |
|---|---|---|---|
| `complaint` | 17,571,890 | one complaint (`complaint_id`) | `year` (2011–2026) |
| `dicionario` | 587 | one published value of a categorical column | none |

Coverage: complaints received **2011-12-01 to 2026-09-05**. The source snapshot used
for the initial load carries `Last-Modified: 2026-09-05 09:22 UTC`.

## Source

Only the **bulk export** is usable:

    https://files.consumerfinance.gov/ccdb/complaints.csv.zip     (~1.4 GB zip, 9.3 GB CSV)

It is a **full snapshot of the whole database, refreshed daily**.

The documented REST/CSV API at `www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
is **behind Akamai and returns "Access Denied" to every scripted client**, regardless
of user agent. Since release 23 (July 2026) its filtered CSV export is also capped at
100,000 complaints and its JSON export has been discontinued. Do not build against it.

Licence: US federal government work, not subject to copyright (17 U.S.C. § 105); the
CFPB states the data is "freely available for anyone to use, analyze, and build on".
Recorded as `cc0`, matching `us_cfpb_hmda`.

## The export lost two columns in June 2026

The current export has **16 columns**. Per the CFPB
[release notes](https://cfpb.github.io/api/ccdb/release-notes.html):

- **Release 22 (June 2026)** removed `Consumer disputed` and `Consumer consent provided`
  from exports (consumer consent had already gone in September 2025).
- The consumer dispute process itself was discontinued on **24 April 2017**.

Neither field is recoverable from the bulk export, so neither is in the table. Any
plan or prompt asking for a "consumer-disputed flag" predates this change.

## The taxonomy has three generations — never remap

The complaint form's product / sub-product / issue / sub-issue options were revised in
**April 2017** and again in **August 2023**. Values are stored exactly as the CFPB
published them. The same product family therefore appears under successive labels with
disjoint coverage:

| `product` | Coverage |
|---|---|
| `Credit reporting` | 2012–2017 |
| `Credit reporting, credit repair services, or other personal consumer reports` | 2017–2023 |
| `Credit reporting or other personal consumer reports` | 2023– |

`Bank account or service` → `Checking or savings account`, `Consumer Loan` →
`Vehicle loan or lease` / `Payday loan…`, and `Credit card` / `Credit card or prepaid
card` / `Prepaid card` behave the same way.

The `dicionario` table records the observed year range of every value in
`cobertura_temporal`. The two authoritative option-tree PDFs are in the table's
auxiliary-files bundle.

## Consumer narratives

`consumer_complaint_narrative` **is included**, verbatim. The CFPB publishes it only
where the consumer consented and only after removing the personal information it
identifies, replacing it with runs of `X`.

- Present on **3,847,965 complaints (21.9%)**; 3.9 billion characters.
- Only **2.4% of 2026 complaints** carry one — scrubbing and release lag the complaint
  by months. A low rate in a recent period is publication lag, not absence.
- A scan of all published narratives for identifying patterns found roughly **0.013%**
  with residue: 314 email addresses (nearly all corporate support addresses), 110
  phone numbers (nearly all company toll-free lines), 64 runs of 12+ digits (mostly
  tracking/case numbers), 8 strings in `NNN-NN-NNNN` SSN format, 7 runs of 16 digits.
  The text is reproduced as published; Data Basis applies no further masking.

## Derived columns

Only two columns are not straight from the source:

- **`year`** — year of `date_received`, the partition column.
- **`state_id`** — two-digit FIPS, derived from the published `State` abbreviation via
  `basedosdados.br_bd_diretorios_us.state`, which is keyed on `id_state`. NULL for the
  military post codes `AA`/`AE`/`AP` (4,327 rows) and for rows with no state (62,611).
  The published value stays untouched in `state_abbreviation`, including the territory
  the CFPB spells out as `UNITED STATES MINOR OUTLYING ISLANDS`.

`state_abbreviation` carries **no** directory FK on purpose: the directory's primary
key is the FIPS code, and a link must target a directory's primary key.

## Source quirks, preserved

- `date_sent_to_company` precedes `date_received` on 7,050 rows (0.04%).
- `zip_code` is masked where the postal area has ≤20,000 inhabitants: 1,130,458 rows
  carry only a 3-digit prefix + `XX`, 138,969 are `XXXXX`, ~65 are malformed
  (`9999-`, `*9999`, …).
- `company_name` has 8,101 distinct spellings and no stable company identifier, so one
  institution appears under several names over time. A company dimension table would
  be real work and is **not** attempted here.
- `issue` is NULL on 6 rows; `company_response_to_consumer` on 21.

## Code (`code/`, run with the shared venv interpreter)

Run from `code/`; scratch defaults to `~/Downloads/us_cfpb_complaints_data`
(`CFPB_COMPLAINTS_DATA_DIR` overrides).

| Script | What it does |
|---|---|
| `gen_architecture.py` | Writes `sheet_complaint.tsv` / `sheet_dicionario.tsv` — the source of truth |
| `common.py` | Paths, architecture parsing, USPS→FIPS state crosswalk |
| `download.py` | Downloads + unzips the bulk export |
| `clean.py` | Streams the CSV → all-STRING parquet, hive-partitioned by year |
| `gen_dicionario.py` | Builds `dicionario` from the cleaned parquet |
| `verify_parquet.py` | Asserts all-STRING schema and architecture column order |
| `null_proportions.py` | Non-null rates per column per year, from parquet footers (no BQ scan) |
| `upload.py` | Creates the EXTERNAL staging table and uploads |
| `upload_resume.py` | Resumable, size-verified re-upload of the GCS prefix |

### Why the CSV is parsed with Python's `csv`, not DuckDB

Narratives contain newlines inside quoted fields. DuckDB's **parallel** reader
desynchronises on them and raises `CSV Error on Line: 4540023 … Expected Number of
Columns: 16 Found: 1`. The file is not malformed: a strict RFC4180 pass over all
17,571,890 records found every record at exactly 16 fields, zero duplicate
`Complaint ID`, zero malformed rows. Do not "fix" this with `ignore_errors=true` —
that silently drops records.

### Why staging parquet is all-STRING

`pipelines.utils.gcs.dump_header` stringifies the header BigQuery infers the staging
schema from, so typed parquet is rejected once the recurring pipeline writes to the
same staging dataset. The dbt model `safe_cast`s every column. `verify_parquet.py`
enforces this; a green dev run cannot catch a violation.

### Why `upload.py` uses `_upload_to_gcs`

It is the same helper the flow calls, so staging ends up **EXTERNAL** over
`gs://…/staging/us_cfpb_complaints/<table>/*`. A `load_table_from_uri` bootstrap would
leave a **NATIVE** table, which ignores every file a later pipeline run writes — dbt
would serve this bootstrap snapshot forever, with no error and no failing test.
`dump_mode="append"`, never `"overwrite"`: overwrite calls `tb.delete(mode="all")`,
which drops the production table too.

## dbt

`not_null_proportion_multiple_columns` is scoped with `where: __most_recent_year_en__`
— unscoped it reads every column, including the 3.9 GB narrative, at **compile** as
well as test time. `tags` and `consumer_complaint_narrative` are exempted: both are
legitimately sparse in recent years.

## Planned recurring pipeline (not built yet)

Daily source; full-snapshot export. The intended shape:

- Download the snapshot, clean it, and **append only complaints whose `complaint_id`
  is new** — the table is not rebuilt from the snapshot.
- **An update path for recent partitions is mandatory, not optional.** Existing rows
  change: `company_response_to_consumer` moves off `In progress`, `timely_response`
  resolves, `company_public_response` is published within 180 days, and narratives are
  added months later (hence the 2.4% figure for 2026). Re-cleaning and replacing the
  current and previous year's partitions on each run covers all four.
- Daily cadence ⇒ **BD Pro rolling window** applies to `complaint` (`PartBdpro`);
  `dicionario` has no date column and takes no coverage spec. The pro Coverage
  (`is_closed=True`) must exist **before** the spec is switched or the run hard-fails
  at `assert_coverage_topology`.
- Verify a manual dev run (`materialize_to_prod: False, update_metadata: False,
  force_run: True`) before arming.
