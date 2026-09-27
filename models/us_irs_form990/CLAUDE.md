# us_irs_form990 — IRS Form 990 series

Annual information returns filed by US tax-exempt organizations (Form 990 and
990-EZ), the Exempt Organizations Business Master File (the nonprofit
registry) and the automatic revocation list. Publisher: Internal Revenue
Service (IRS), Tax Exempt and Government Entities division.

- **Licence:** US Government work, public domain (17 U.S.C. §105). Registered
  as `cc0` (the closest licence slug the backend carries).
- **Cadence:** e-file ZIPs land in irregular batches through the year; the
  BMF and the revocation list are monthly. A Prefect flow
  (`pipelines/datasets/us_irs_form990`) polls twice a month.

## Tables

| Table | Grain | Source |
|---|---|---|
| `organization` | EIN × monthly extraction (stacked snapshots) | EO BMF regional CSVs `eo1..eo4`, `eo_pr`, `eo_xx` |
| `return_financial` | EIN × tax year × form type (990 / 990EZ) | e-file XML, header + Part I via the concordance |
| `compensation` | return × listed person (Part VII Section A / EZ Part IV) | e-file XML |
| `revocation` | EIN × revocation date, full replacement | `data-download-revocation.zip` |
| `dicionario` | code → label | eo-info.pdf code tables + NCCS NTEE |

## Decisions that are not obvious from the code

- **The concordance is the parser.** Every XML value is located through the
  Nonprofit Open Data Collective master concordance (`ef2`, MIT), trimmed to
  the 92 variables used (`pipelines/datasets/us_irs_form990/concordance.csv`,
  built by `code/build_concordance.py`). No XPath is hand-written. This is why
  **990-PF and 990-T are out of scope**: the concordance covers form types
  PC and EZ only. `form_type` is designed so PF can be added later.
- **Release year ≠ tax year.** IRS ZIPs are keyed by the year the IRS posted
  them (2017–2026 hosted; 2017–2018 are unlinked but still served); each holds
  several tax years. The tables partition on the return's own `year` (TaxYr).
- **Idempotent batches.** Parquet parts are named after the ZIP
  (`<batch>_p<k>.parquet`), so re-processing a batch overwrites itself. The
  dbt model keeps one return per (ein, year, form_type) — latest
  `return_timestamp`, then `object_id` — so amended returns and re-released
  filings never duplicate. `compensation` is restricted to those kept filings.
- **Checkboxes** absent from the XML are `false` on Form 990; the 990-EZ has
  no position checkboxes, so they are `null` there. `organization_type` and
  `exempt_status` are derived from the checkbox groups.
- **One Deflate64 ZIP** (`2020_TEOS_XML_CT1.zip`) is unreadable by Python's
  `zipfile` and has 76 stray bytes; `utils._iter_members` falls back to
  Info-ZIP `unzip`, ignores its exit code and checks the member count.
- **State columns carry no backend directory link.** They hold USPS
  abbreviations, the US state directory is keyed on FIPS (`id_state`), and the
  backend accepts a directory link only to a table's primary-key column. The
  referential check is a dbt `custom_relationships` test against the
  directory's `abbreviation` column instead, with a small tolerance for
  foreign addresses. `br_bd_diretorios_us` is the GCP dataset id; the backend
  slug is `diretorios_us`.
- **BMF snapshots** stack on `extraction_date` (the `Last-Modified` date of
  `eo1.csv`); the IRS does not archive past extracts, so the panel starts at
  the first onboarded month (2026-08-10).
- **All tables are free** for now. Paywalling the BMF's newest window
  (`PartBdpro`) needs several snapshots first, or the free range inverts.
- `dump_mode="append"` everywhere: `overwrite` drops the prod table even from
  a dev run.

## Coverage caveats (in the table descriptions, deliberately)

- **Release year 2022 is one third short at the source.** `index_2022.csv` lists
  515,245 Form 990/990-EZ filings but `2022_TEOS_XML_01A.zip` holds 339,356 of
  them; the 175,889 index rows without an `XML_BATCH_ID` are in no IRS ZIP at
  all, and none surfaced in later batches. This is an IRS-side gap, not a
  parser gap (checked object id by object id).

Paper filers are absent from the e-file feed. E-filing became mandatory for
all Form 990 filers only for tax years beginning after 1 July 2019, so tax
years before 2020 under-count small organizations.

## Layout

```
code/
├── architecture.py         column spec (EN/PT/ES) → architecture/*.csv, columns_json/*.json
├── tables.json             table names and descriptions (EN/PT/ES), keys
├── build_concordance.py    trims the NODC concordance into the pipeline package
├── build_dicionario.py     code tables → dicionario.csv (ntee_codes.csv = NCCS labels)
├── build_dbt.py            architecture → *.sql + schema.yml (measures sparse columns)
├── build_auxiliary_files.py per-table bundles → GCS
├── clean.py                one-shot bootstrap over pipelines/datasets/us_irs_form990/utils.py
└── upload.py               streaming GCS upload + BigQuery load (dev)
```

Scratch data lives in `~/Downloads/us_irs_form990_data/` and is deleted at the
end of the onboarding.
