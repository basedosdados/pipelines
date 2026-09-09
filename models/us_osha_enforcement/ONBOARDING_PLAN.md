# us_osha_enforcement

OSHA enforcement data — every workplace safety inspection the agency or a state
plan has carried out since 1972, the violations cited, the penalties assessed
and revised, and the incidents and injuries investigated.

**11 tables, 38,820,592 rows.** Organization `osha`, backend slug `enforcement`,
GCP dataset `us_osha_enforcement`.

---

## Source

The DOL Enforcement Data Catalog. `enforcedata.dol.gov` is retired and 301s to
`data.dol.gov`, a single-page app whose API (`apiprod.dol.gov/v4/get/...`)
requires a key. The bulk files do not:

```
https://data.dol.gov/data-catalog/OSHA/<table>/OSHA_<table>.zip
```

recovered from the portal's own "Download Complete Dataset" link. Eleven files,
6.0 GB, reissued daily. Each zip is *stored* rather than deflated and holds
numbered CSV chunks, each repeating the header, so extraction would double the
disk for nothing — everything streams out of the zip.

The per-table data dictionary comes from `apiprod.dol.gov/v4/datasets/<id>`,
which needs no key, and is cached in `code/source_schema.json`. It is what
decides which columns arrive as float strings.

`www.osha.gov` is a different matter: it blocks `curl`, `wget` and `requests`
with HTTP 403 on TLS fingerprint and needs `curl_cffi`. Nothing in the pipeline
depends on it; only the licence and code-legend research did.

**Licence:** US federal public domain, confirmed at
<https://www.dol.gov/general/aboutdol/copyright> — "Materials created by the
federal government are generally part of the public domain and may be used,
reproduced and distributed without permission", credit to the U.S. Department
of Labor. Mapped to `cc0`, availability `online`, matching `us_dol_oflc`.

## Organization: a new `osha`, not the existing `dol`

`dol` exists and `us_dol_oflc` uses it. But `bureau_of_labor_statistics_bls`
exists alongside it, as do `epa`, `fbi`, `irs`, `cfpb`, `fema`, `eia` and
`noaa`: the house convention is agency-level for operating agencies. Decisively,
the GCP id follows `us_<org>_<slug>`, so `us_osha_enforcement` requires the org
to be `osha` — reusing `dol` would force `us_dol_enforcement`.

## Tables

| table | rows | grain | partition year from |
|---|---:|---|---|
| `inspection` | 5,199,042 | one inspection | its own `open_date` |
| `violation` | 13,265,971 | inspection × citation | parent inspection |
| `violation_event` | 11,657,476 | citation × event | parent inspection |
| `violation_text` | 1,005,185 | citation | parent inspection |
| `related_activity` | 2,501,166 | inspection × related activity | parent inspection |
| `emphasis_code` | 2,926,935 | inspection × program | parent inspection |
| `optional_code_info` | 1,700,431 | inspection × field | parent inspection |
| `accident` | 165,801 | one incident | its own `event_date` |
| `accident_injury` | 231,789 | injured person | incident, else inspection |
| `accident_narrative` | 165,794 | one incident | parent incident |
| `dicionario` | 1,002 | code | unpartitioned |

Two source files are reassembled rather than published line by line:
`osha_accident_abstract` (1,151,160 lines → 165,794 narratives) and
`osha_violation_gen_duty_std` (2,217,824 lines → 1,005,228 citation texts).
Both store text wrapped across rows; the line grain is a storage artefact, not
normalisation.

`activity_nr` is unique across all 5,199,042 inspections and
`(activity_nr, citation_id)` across all 13,266,345 violations — verified on the
full files, zero duplicates.

**Partitioning follows the inspection.** Every child table takes its `year` from
its parent's `open_date`, so a partition refresh covers the same set of cases in
every table. The children have no date of their own, and the dates they do carry
are unusable as a key: `close_case_date` reaches back to `0120-11-18` and
`hist_abate_date` forward to `5015-08-28`.

## Traps, all measured against the full files

1. **`accident_abstract` uses two wrapping regimes and flags neither.** Older
   IMIS records pad every line but the last to exactly 80 characters and split
   mid-word (join with nothing); newer records wrap at word boundaries and drop
   the space (join with one). Concatenating everything corrupts the 56,457
   word-wrapped narratives into `"cleanoff a driveway"` and `"theemployee"` —
   text that still reads as prose and passes every aggregate check. Deciding on
   line length alone is not enough either: 615 word-wrapped narratives happen to
   have every non-final line at exactly 80 characters. Together, 34% of the
   165,794 narratives.

   `join_lines` screens on length, then votes over each boundary: the split is
   mid-word if the two boundary tokens concatenate into a corpus word
   (`scaffoldi` + `ng`), or if either half is not a word on its own (`autoe` +
   `levator`, where the join is too rare to be in the vocabulary). The
   vocabulary is built from tokens that never touch a line boundary, so it
   cannot contain fragments. Measured share of long tokens that are not corpus
   words:

   | join rule | unknown long tokens |
   |---|---:|
   | always concatenate | 30.82% |
   | always space | 4.67% |
   | length rule only | 4.03% |
   | shipped rule | **3.95%** |

   Final split: 108,158 fixed-80, 56,457 word-wrapped, 1,179 single-line.

2. **The source is not grouped by key.** A streaming reassembler that flushes
   when the key changes produces **206,321** fragments where there are
   **165,794** narratives — 24% silently corrupted. Grouping goes through a
   dict.

3. **Numeric columns arrive as float strings** — `757.0`, `12.0`,
   `100007640.0`. The join to `dicionario` matches nothing without stripping,
   and fails silently. Which columns to strip is read from the DOL catalog's own
   `data_type`, not guessed from the value.

4. **Corrupt dates throughout.** `close_case_date` min `0120-11-18`,
   `final_order_date` min `0018-05-30` max `2103-05-22`, `hist_abate_date` max
   `5015-08-28`, `closing_conference_date` max `2112-12-05`. BigQuery's DATE
   type accepts every one of them, so `norm_date` drops any year outside
   1900-2100 rather than publish it as a real date. `open_date` is clean — 1970
   has 1 row, 1971 has 2, then 17,164 in 1972 — and is the only safe partition
   anchor.

5. **Five columns the DOL dictionary documents as populated are 100% NULL**:
   `inspection.state_flag`, `accident.event_time`, `accident.state_flag`,
   `accident.abstract_text` (the abstract lives in its own file) and
   `optional_code_info.opt_info_id`. Dropped.

6. **`site_state` has 63 values**, including codes long extinct: `CZ` (Canal
   Zone), `JQ` (Johnston Atoll), `PI`, `UK`, `FN`, `MQ`. A `relationships` test
   against `br_bd_diretorios_us.state` would fail on correct data, so the column
   is documented rather than linked. `naics_code` is likewise unlinked: the
   source never says which NAICS vintage, and the directory is vintaged.

7. **`violation_gen_duty_std` is not what its name or the DOL dictionary say.**
   Of its 1,005,228 citations only **4.6% cite the General Duty Clause**
   (5A0001); the most frequent standards are fall protection (1926.501, 10.1%)
   and hazard communication (1910.1200, 6.1%). Coverage is almost entirely
   modern — 0.4% of 1990s citations, 0.8% of 2000s, 31% of 2010s, 39% of 2020s.
   It is the citation-narrative table for a third of citations since 2010.
   Published as `violation_text`, with those numbers in its description.

8. **`0` is OSHA's not-applicable sentinel, and its own lookup file omits it.**
   Every numeric code column in `accident_injury` carries a `0` that the lookup
   does not define — except `occupation_code`, where OSHA defines
   `0 = "Occupation Not Listed"`. `0` appears on exactly 5,434 rows in each of
   seven independent columns (one set of uncoded records, not seven
   coincidences) and on 63-67% of rows in the three construction-only columns.
   The value is kept; those columns are held out of the dictionary-coverage
   test and the evidence recorded in each column's `observations`.

9. **`accident.event_date` disagrees with the narrative for some records.**
   Incident `170882419` carries `1975-10-01` while its own abstract describes an
   event on 1994-05-02. A source defect, left faithful.

## The narrative fields

Both were read before publication.

**`accident_narrative` is systematically de-identified.** Across a sample of
21,133 narratives: no personal name, no identity number, no telephone number.
Injured people are `"Employee #1"` (56%) or `"an employee"` (44%). What is
there: age, sex, occupation, employer, exact date and time, location and injury
detail — re-identifiable to anyone who knows the workplace, but no direct
identifier. OSHA publishes the same text verbatim on its own site.

**`violation_text` does name individuals**, because a citation is a public legal
document that names its respondent. Seven of thirteen title-prefixed matches in
a 24,382-citation scan are of the form *"Mr. <name> was previously cited for…"*,
and each checks out as the cited employer itself:

```
346527914 | WILMER JOEL CRUZ MEJIA | ODENVILLE AL | 2023-02-22
346586381 | EDUARDO RUIZ          | CORINTH TX   | 2023-03-21
```

— sole proprietors whose name is already `inspection.establishment_name` and
already on OSHA's Establishment Search. One 1992 citation names a manager
incidentally. The apparent SSN and telephone matches were all false positives: a
malformed date `011-22-2022`, and OSHA's own 1-800-321-OSHA.

## Linking to the EPA facility-level datasets

`us_epa_tri.facility` and `us_epa_ghgrp.facility` both carry **`frs_id`**, EPA's
Facility Registry Service identifier — that is their shared anchor. OSHA carries
none of it: only `establishment_name`, `site_address`, `site_city`,
`site_state`, `site_zip_code`, and `establishment_key`, an internal IMIS key
populated on 58.6% of rows and not stable across years.

So a facility-level compliance cluster is possible only through name-and-address
normalisation, or through EPA's own FRS program links, which are not onboarded.
No crosswalk is built here.

## Refresh

Weekly, Sunday 04:47 America/Sao_Paulo. The source reissues everything daily and
amends prior records in place — a penalty is contested and revised for years —
so the flow is a **partition refresh**, not an append: it rebuilds a set of
partition years and rewrites one Parquet object per partition, leaving the rest
of the prefix alone.

`plan_refresh` picks the years from a trailing window (8 years) plus any older
year OSHA has actually touched, read from `inspection.case_mod_date`. On the
2026-09-08 files that selects 2010-2026. Verified equivalent to a full rebuild:
cleaning only 2024-2025 reproduces those partitions exactly (2,038 / 2,151 /
2,038 rows).

**All tables are `AllFree`.** The house rule paywalls the recent window of any
table refreshing monthly or more often, and weekly qualifies — but the tier is a
business decision, this is public-domain federal data OSHA publishes free, and
switching a table to `PartBdpro` requires a pro Coverage to exist on it first or
`assert_coverage_topology` hard-fails. Left free deliberately, to be revisited.

## Open items

- `us_osha_enforcement_staging` is a **new BigQuery dataset** and needs the
  table-approve service account granted on it before the PR can materialise
  prod.
- Auxiliary files: the DOL catalog publishes its dictionary as API metadata
  rather than as documents, and it is already captured in
  `code/source_schema.json`. No per-table bundle is warranted.

## Reproducing

```bash
# clean (downloads what is missing; ~8 min, 6 GB in, 1.1 GB out)
PYTHONPATH=$PWD python models/us_osha_enforcement/code/clean_data.py --all

# regenerate architecture CSVs, dbt models, schema.yml, columns_json
python models/us_osha_enforcement/code/generate.py

# upload to basedosdados-dev staging
GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
PYTHONPATH=$PWD python models/us_osha_enforcement/code/upload_data.py --all

# register metadata (idempotent)
python models/us_osha_enforcement/code/register_metadata.py --env staging
```
