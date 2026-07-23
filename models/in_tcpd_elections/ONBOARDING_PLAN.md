# Onboarding plan — Lok Dhaba (TCPD) Indian elections — `in_tcpd_elections`

All identifiers in **English** (tables, columns, directory). Reconcile the dataset slug with the
existing **prod** stub `1b0856e3-8b76-46eb-b5ff-418763669fcf` at the discover step; rename if needed.

## Source & citation (dual)

- Portal: https://lokdhaba.ashoka.edu.in/browse-data · Producer: Trivedi Centre for Political Data (TCPD), Ashoka University.
- The single Lok Dhaba download **merges two separately-cited TCPD datasets** at one row grain, so both are registered as raw data sources:
  - **TCPD-IED** — "TCPD Indian Elections Data v2.0" (base ECI results).
  - **TCPD-IID** — "TCPD Individual Incumbency Dataset, 1962-current" (derived career vars + `pid`).
- Contributing: ADR / MyNeta (education, profession). Excluded: DataMeet spatial (maps; geometry out of scope v1).
- License: free non-commercial with attribution, no warranty. Version pinned: files dated **2023-06-02**.
- Codebook: https://lokdhaba.ashoka.edu.in/static/media/2022Feb12LokDhabaCodebook.21040cf7.pdf

## Download (programmatic)

```
https://lokdhaba.ashoka.edu.in/downloads/All_States/All_States_GE.csv.gz   # 3.3 MB, 91,669 rows
https://lokdhaba.ashoka.edu.in/downloads/All_States/All_States_AE.csv.gz   # 17 MB, 483,565 rows
```

Grain: one row per candidate × constituency × election instance. Verified unique key (0 dups):
`(state, assembly_no, constituency_no, year, month, poll_no, position)`.
Coverage: GE 1962–2021 (40 states incl. erstwhile); AE 1961–2023 (34 states). IED and IID share this
exact observation level (IID adds columns + `pid`, no extra rows) — hence merged tables.

## Tables (English)

- `in_tcpd_elections.general_elections` — 44 cols (Lok Sabha / GE)
- `in_tcpd_elections.assembly_elections` — 46 cols (adds `candidate_age`, `district_name`)
- `in_tcpd_elections.dictionary` — value→label for the 5 coded columns (cols: table_id, column_name, key, temporal_coverage, value)
- `br_bd_diretorios_in.state` — state/UT directory (cols: acronym, name, iso_3166_2, type, is_current [BOOL], successor_acronym); FK key is `acronym`

`Election_Type` dropped (constant per table). Partition by `year` (INT64), range start=1961 end=2028.
`year`/`month` link to `br_bd_diretorios_data_tempo`; `state_acronym` links to `br_bd_diretorios_in.state:acronym`.
7 flag columns are `BOOL` (deposit_lost, incumbent, recontest, last_poll, turncoat, same_constituency, same_party).

**Authoritative column definitions are the CSVs in `architecture/`** (name, bigquery_type, description[pt],
covered_by_dictionary, directory_column, measurement_unit, has_sensitive_data, observations, original_name).
Reference tables are embedded in `code/reference.py` (state directory 40 rows, raw→acronym crosswalk 41,
dictionary values) since the repo gitignores `**/data/`. `candidate_id` = TCPD `pid`: STRING, `covered_by_dictionary=no`, no directory,
null on NOTA, version-scoped. The 5 dictionary-covered columns: poll_type, delimitation_id,
constituency_type, candidate_sex, candidate_type. Column descriptions carry no trailing period.

## Cleaning rules (data-quality issues found)

- `candidate_sex`: uppercase; `male`/`MALE`→`M`, `female`/`FEMALE`→`F`, `THIRD`→`O`; junk (`(SC)`,`ST`,`SC`,`E`,`(E)`,`NOTA`)→null.
- `candidate_type`: uppercase; `GENERAL`→`GEN`; keep GEN/SC/ST; `NOTA`/`OBC`/`BL`/blank→null (matches dictionary).
- `constituency_type`: keep GEN/SC/ST; `BL`→null.
- `state_acronym`: map raw `State_Name` → `acronym` via `data/state_crosswalk.csv` (two Goa spellings → GDD).
- `candidate_id`: blank on NOTA → null (expected).
- `turnout_percentage` (>100% & negative) and `candidate_age` (0, 333): preserve source values; document ranges; relax dbt tests.
- BOOL columns: `TRUE`/`FALSE` and `deposit_lost` `yes`/`no` → nullable boolean; blank→null.

## Steps

1. context ✓ 2. architecture (4 sheets from `architecture/` CSVs) **[commit]** 3. download→`input/`
4. clean → parquet + directory + dictionary **[commit after user verifies subset]** 5. upload dev (staging)
6. dbt 7. validate **[commit]** 8. discover (+reconcile slug) 9. metadata dev **[PAUSE]** **[commit]**
10. metadata prod (after approval) **[commit]** 11. PR.
