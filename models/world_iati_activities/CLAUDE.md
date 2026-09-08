# world_iati_activities

Activity, transaction and results data published to the **International Aid
Transparency Initiative** (IATI) by development and humanitarian organisations.

## Where the data comes from, and why not from somewhere else

IATI has five access routes. This dataset uses the third.

| Route | What it gives | Why not it |
|---|---|---|
| `dashboard.iatistandard.org` | Metrics *about publisher behaviour*, not aid flows | Different dataset. Its daily history is also frozen: 62 of its 76 dated series stopped on 2024-02-20, two on 2017-01-18 |
| Bulk Data Service (`bulk-data.iatistandard.org`) | The raw XML corpus, 832 MB zipped | Would mean writing and maintaining our own IATI XML flattener |
| **IATI Tables** (`data.tables.iatistandard.org`) | **193 relational tables, 141.9M rows, rebuilt daily from the Bulk Data Service** | **Used** |
| Datastore v3 | Activity/budget/transaction search | Needs an API key, and is paginated rather than bulk |
| CDFD (`countrydata.iatistandard.org`) | 283 country workbooks with transactions pre-split | Derived, .xlsx-only, and redundant with `transaction_breakdown` |

`iati-data-dump.codeforiati.org` was deprecated in October 2025 and now points at
the Bulk Data Service.

Of IATI Tables' 193 tables we take 18. The rest are `*_narrative` side-tables
whose content IATI Tables has already flattened into the parent, and
publisher-specific namespace extensions (`transaction_usg:treasuryaccount*`,
`budget_gavi:*`, and the `xmlns:*` columns) — about 11M rows belonging to two
publishers. A 19th table, `registry_dataset`, is ours.

## Licence: per publisher, not per corpus

IATI's own terms: data "is the property of its original reporting organisation
and is released in line with the open license as listed on the IATI Registry"
(https://web-terms.iatistandard.org/en/latest/copyright/). Only IATI's written
material is CC-BY-SA 4.0; the data itself has 13,901 separate licence
declarations.

Measured over the whole registry index:

```
cc-by 33.1% · other-at 16.9% · cc-zero 15.5% · other-open 10.2% · odc-by 9.7%
odc-pddl 3.4% · other-pd 2.6% · uk-ogl 1.5% · cc-by-sa 1.5% · odc-odbl 0.7%
non-commercial 0.12% (cc-nc 13, other-nc 3) · undeclared 4.4% (396 blank + 220 notspecified)
```

The transform therefore does two things no upstream tool does:

1. joins `licence_id` onto **every row**, from the Bulk Data Service index
   (`short_name` → IATI Tables' `dataset`), so a user can filter further;
2. drops the 16 non-commercial datasets, and drops rows whose registry dataset
   is no longer in the index at all — for those there is no licence we can
   assert. `registry_dataset` keeps all 13,901 rows so what was excluded stays
   visible.

## Source traps the transform exists to neutralise

Each of these would have become a silently NULL column rather than an error,
which is why they are handled in the parquet and not left to `safe_cast`.

1. **Booleans are PostgreSQL's `t`/`f`.** `SAFE_CAST('t' AS BOOL)` is NULL, not
   an error. Every boolean column in the dataset would have come out empty.
   Normalised to `true`/`false`.
2. **Timestamps carry mixed UTC offsets** — `+00:00`, `Z`, `+01:00`, `+02:00`,
   `-04:00`, `-05:00`, and 74 values with no offset at all. `SAFE_CAST` of an
   offset-bearing string to DATETIME is NULL. Converted to UTC and rendered
   `YYYY-MM-DD HH:MM:SS`; the offset is dropped, not preserved.
3. **`transaction_breakdown` has no `dataset` column** — only `prefix`. Its
   licence is reachable only by joining `_link_transaction` back to
   `transaction`. A publisher can use more than one licence, so `prefix` is not
   a substitute.
4. **The Datasette instance calls the transaction table `trans`** (`transaction`
   is reserved in SQL); the CSV and pg_dump exports call it `transaction`. Key
   on the export name.
5. **Dates are whatever the publisher typed.** Across 11.56M transactions: 566
   with no date, 0 unparseable, 2 outside 1900–2100 (the observed extremes are
   year 5 and year 2107). Rows outside that window, or with no date, go to
   partition `year = 0` rather than being dropped or given an invented date —
   Hive-partitioned NULLs land in `__HIVE_DEFAULT_PARTITION__`, which BigQuery
   cannot read as INT64.
6. **`reportingorg_type`/`typename` on `activity`, not `_code`/`_codename`.**
   Most IATI Tables codelist pairs use the `_code`/`_codename` shape; this one
   and several others (`type`/`typename`, `role`/`rolename`,
   `status`/`statusname`) do not. Validating all 335 mapped columns against the
   real CSV headers caught it, along with `transaction_breakdown` carrying
   `value_currency` but no `value_currencyname`.

## What this dataset is not

**It is a current snapshot, not a vintage series.** IATI publishers restate
history freely, and IATI Tables replaces its entire corpus each run — updates
and removals are both respected. Nothing here records what the data said last
month. A refresh rewrites past years as readily as the current one.

That also means the headline counts are not a growth series. Activities in the
corpus went 1,064,203 (2022-01-01) → 805,187 (2024-01-01) → 904,495
(2026-09-07). The falls are publishers deregistering and files going
unreachable, not aid stopping.

`activity_id`, `transaction_id` and every other `_link`-derived key are
synthetic and **not stable across runs**. Use `iati_identifier` to follow an
activity over time.

## Layout

```
models/world_iati_activities/
├── code/
│   ├── common.py             scratch paths (~/Downloads/world_iati_activities_data)
│   ├── gen_architecture.py   writes architecture/*.csv — the schema authority
│   ├── gen_dbt.py            writes the .sql models and schema.yml from those CSVs
│   ├── clean.py              one-shot bootstrap; imports the shared transform
│   ├── verify_parquet.py     column order, all-STRING, uniqueness, empty columns
│   ├── upload.py             parquet → basedosdados-dev staging
│   └── architecture/         one sheet_<table>.csv per table
└── world_iati_activities__<table>.sql

pipelines/datasets/world_iati_activities/
├── constants.py   URLs, table list, partition sources, the NC licence set
└── utils.py       the transform — pure functions, no Prefect
```

The transform lives once, in `pipelines/.../utils.py`; `code/clean.py` imports
it so the bootstrap and the recurring pipeline cannot drift.

## Refresh

IATI Tables rebuilds daily. The pipeline runs **weekly**: there is no
incremental key — a refresh is a full rebuild of the whole corpus — so daily
would buy marginal freshness at seven times the cost. `stats.json`'s
`data_dump_updated_at` is the poll target.
