# us_fema_openfema

Open data from the U.S. Federal Emergency Management Agency, published through
the OpenFEMA program: Stafford Act disaster declarations since 1953, Public
Assistance funded projects since 1998, and the redacted National Flood
Insurance Program claims and policies files.

## The licence is the first thing to know

**OpenFEMA is not public domain.** Every set has `license: null` in the
catalog and is governed by <https://www.fema.gov/about/openfema/terms-conditions>,
which binds users to:

- use the data **solely for statistical research or as a reporting record**;
- **not** use it "to make determinations that might affect an individual's
  rights or eligibility for benefits";
- not reidentify, nor publish facts that may lead to identification;
- **cease use and destroy any copy if FEMA requests it**;
- carry FEMA's disclaimer verbatim and cite the endpoint with a retrieval date.

Registered as licence `unknown`, not `cc0`. **Do not put these tables behind
BD Pro without resolving the licence question first** — the frequency rule
would otherwise make all four `PartBdpro`, and "solely for statistical research"
plus the rights/eligibility ban is a real constraint on a commercial
insurance or climate-risk product. The coverage specs in `flows.py` are
`AllFree` on purpose.

## The NFIP sets were renamed under us

`FimaNfipClaims` v2 and `FimaNfipPolicies` v2 are **deprecated** — frozen at
2026-06-01, removed 2026-10-15. This dataset builds against `NfipClaims` v3
and `NfipPolicies` v3. `check_source_openfema` raises if any pinned set gains
a `depDate`, so the next sweep is caught on the following run.

## Refresh cadence

- `13 5 * * *` — daily at 05:13 America/Sao_Paulo.
- Each run polls every set's **`lastDataSetRefresh`** in OpenFEMA's catalog and
  refreshes only the sets that moved. `lastRefresh` is a schema/store timestamp
  that lags badly and must not be used.
- Declarations refresh every 20 minutes at source, Public Assistance daily,
  both NFIP files monthly.

Staging upload: dump mode `append`, source format `parquet`. Worker: `8Gi`.

## Tables

| table | grain | partition | rows | coverage |
|---|---|---|---|---|
| `disaster_declaration` | declaration × designated area | `year` of declaration date | 70,401 | 1953-05-02 – 2026-09-01 |
| `public_assistance_project` | project | `year` of declaration date | 848,733 | 1998-08-26 – 2026-06-30 |
| `nfip_claim` | claim | `year` of loss | 2,724,656 | 1978-01-01 – 2026-08-03 |
| `nfip_policy` | policy | `year` of effective date | 74,349,525 | 2009-01-01 – 2027-07-17 |
| `dicionario` | table × column × code | — | 784 | — |

All coverage tiers are `all_free`. The 2027 policy rows are forward-dated
policies, which is normal in insurance and not a data error.

## Redaction

FEMA publishes the NFIP files already redacted: latitude and longitude are
rounded to one decimal place and the address is reduced to city, ZIP and
census block group. **Republish exactly that.** Do not geocode, join to a finer
geography, or otherwise sharpen location — that is the reidentification the
terms forbid.

## Where the code lives

- `pipelines/datasets/us_fema_openfema/` — `spec.py` (the table specification:
  renames, keys, partitions, derived columns — the single source of truth,
  shared with the build scripts), `constants.py`, `utils.py` (pure download and
  cleaning transform), `tasks.py`, `flows.py`.
- `models/us_fema_openfema/` — dbt models and `schema.yml`, both **generated**.
  Edit the generator and regenerate; do not hand-edit the outputs.

| script in `code/` | what it does |
|---|---|
| `fetch_source_metadata.py` | refreshes the cached catalog + field dictionary |
| `build_architecture.py` | `architecture/*.csv` and `columns_json/` |
| `build_dicionario.py <out>` | `dicionario.csv`; reports codes with no label |
| `build_dbt.py` | the five `.sql` models and `schema.yml` |
| `build_auxiliary_files.py <out>` | the per-table bundles |
| `validate_types.py <in>` | architecture types vs the shipped parquet |
| `upload.py` | BigQuery dev staging upload |

`code/tables.py` is a re-export shim for `pipelines/.../spec.py`, so the
cleaning transform and the architecture cannot drift apart.

Scratch data: `~/Downloads/us_fema_openfema_data/{input,output}` — never in the
repo or under Dropbox. Override with `US_FEMA_OPENFEMA_DATA`.

## Source

- Catalog and field dictionary (drives the architecture):
  `https://www.fema.gov/api/open/v1/DataSets?$top=1000` and
  `.../DataSetFields?$top=2000`. Cached in `code/source_metadata.json`.
- Bulk parquet, verified 2026-09-06:
  `/api/open/v2/DisasterDeclarationsSummaries.parquet` (7.4 MB),
  `/api/open/v2/PublicAssistanceFundedProjectsDetails.parquet` (119 MB),
  `/about/reports-and-data/openfema/v3/NfipClaimsV3.parquet` (164 MB),
  `/about/reports-and-data/openfema/v3/NfipPoliciesV3.parquet` (3.7 GB).

## Design notes

- **The parquet wins over the field dictionary.** `NfipClaims.dateOfLoss` is
  documented `datetime` but shipped `date32`; disagreements go in
  `spec.TYPE_OVERRIDE` and `validate_types.py` fails the build on a new one.
  Absorbing them with `safe_cast` would silently produce NULLs.
- **`www.fema.gov` stalls on the 3.7 GB policy file**, twice at ~96%, and a
  truncated transfer still returns HTTP 200. `download_table` resumes via HTTP
  Range and accepts the file only once the parquet footer reads.
- **Public Assistance emits FEMA-internal codes** 1001 and 1003 for the
  Northern Mariana Islands and American Samoa instead of FIPS 69 and 60, and
  zero-pads neither its state nor county code. `_check_geography` raises if any
  derived key comes out the wrong width rather than letting it join to nothing.
- **County code 0/00/000 means "statewide"** and becomes NULL, not a county
  that does not exist: 1,610 declaration rows, 19,077 project rows.
- **`census_geoid` is a 12-digit block group, not a tract.** `census_tract_id`
  is its first 11 digits. FEMA does not document the census vintage, so it is
  deliberately **not** linked to `census_tract_2020`.
- `state_abbreviation` carries USPS codes and cannot be linked to the state
  directory, which is keyed on FIPS; the integrity check is a dbt test.
- The wide NFIP tables scope `not_null_proportion_multiple_columns` to the
  newest partition — unscoped it scans 90 columns × 74M rows **at dbt compile**,
  which threatens the project-wide daily byte quota.
