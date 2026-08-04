# au_alexander_politicians

Biographical and political data on Australian **federal** politicians who served
between **1901 and 2021**, from Rohan Alexander & Paul Hodgetts' open dataset.

- **Source:** https://github.com/RohanAlexander/australian_politicians (`data/`), MIT license
- **Coverage:** 1901–2021 (birth dates back to 1829)
- **Organization:** none (individual academic dataset; credited via description + raw source)
- **Status:** frozen upstream since 2021-11-29 — see [UPDATE_MECHANISM.md](UPDATE_MECHANISM.md)

## Tables

| Table | Rows | Grain |
|---|---|---|
| `politician` | 1,783 | one row per politician (master; folds in `id_aph`, `id_wikidata`) |
| `party_affiliation` | 2,264 | politician × party spell |
| `house_member` | 1,430 | politician × House-of-Representatives division spell |
| `senator` | 696 | politician × Senate state spell |
| `ministry` | 2,920 | politician × portfolio within a ministry |
7 source CSVs → 5 tables: `uniqueID_to_aphID` is folded into `politician`;
`uniqueID_to_aph_ministries` (an internal name-matching build artifact) is dropped.
The `indicator_*` flags are stored as native `BOOLEAN`, so no dictionary table is needed.

## Foreign keys / directories

- **State** — `house_member.id_state` and `senator.id_state` link to
  `br_bd_diretorios_au.state` (`id_state`). The 2-letter source abbreviation is
  mapped to the ASGS state code (NSW→1 … ACT→8) and kept alongside as
  `abbreviation_state`.
- **Politician** — every satellite table's `id_politician` links back to
  `politician` (enforced by a dbt `relationships` test).
- **Electoral division** (`house_member.division`) — kept as a plain STRING, **no
  hard FK**: it includes abolished/renamed historical seats that predate the
  directory's 2016/2021 `commonwealth_electoral_division` snapshots.

## Reproduce

```bash
cd models/au_alexander_politicians/code
python architecture/gen_architecture.py   # (re)write architecture CSVs (source of truth)
python clean.py                           # download source, write output/*.parquet
python gen_dbt.py                         # (re)write ../*.sql + ../schema.yml
python upload.py                          # -> basedosdados-dev staging (needs BD dev creds)
```

Then from the repo root: `dbt run --profiles-dir . --select au_alexander_politicians`.

## Cleaning notes (source quirks handled)

- `indicator_*` flags stored as native `BOOLEAN`. Sparse "1-or-blank" flags (PM,
  changed-seat, etc.) filled to `false`; `enteredAtByElection` normalised from mixed
  `1`/`Yes`/`No`; genuinely missing flags (`indicator_section_15_selection`) stay NULL.
- `Connelly` (bare surname in `mps-by_division` and the aphID crosswalk) remapped
  to the master id `Connelly1978` (verified same person: Vince Connelly, Stirling
  WA, entered 2019-05-18).
- 5 aphID-crosswalk ids have no master `politician` record and are dropped from the
  fold; `ministry` keeps 2 source rows with a null `id_politician`.
- End-reason labels are kept verbatim (minor source spelling inconsistencies).
