# br_bd_diretorios_au — Australian geography directory

Directory (diretório) of Australian geographic and administrative units, the
Australian analogue of `br_bd_diretorios_us`. All content derives from the
**Australian Statistical Geography Standard (ASGS)** published by the Australian
Bureau of Statistics (ABS), redistributed under **CC BY 4.0** (attribution:
Australian Bureau of Statistics).

## Scope

Two ASGS editions as paired, `_<year>`-suffixed snapshots — **2016** (Edition 2)
and **2021** (Edition 3) — plus two ABS correspondence (crosswalk) tables. State
codes are fixed across editions, so `state` is unstamped. 23 tables total.

| Group | Tables |
|---|---|
| Unstamped | `state` |
| Per edition (×2016, ×2021) | `sa1`, `sa2`, `sa3`, `sa4`, `gccsa`, `lga`, `postal_area`, `suburb`, `commonwealth_electoral_division`, `state_electoral_division` |
| Crosswalks | `correspondence_sa2_2016_2021`, `correspondence_lga_2016_2021` |

Each directory table carries the unit's own `id`/`name`, its full parent chain
(codes + names), a derived state `abbreviation`, and `area_albers_sqkm` (km²,
Albers equal-area). SA1 has no name (ABS assigns none). `postal_area` has no
state (POAs cross borders). Crosswalks carry `ratio` (population-weighted
apportionment) and a `quality_indicator`.

### Change over time

The genealogy question is answered two ways: diff the paired snapshots
(`lga_2016` vs `lga_2021`), or use the ABS correspondence tables to apportion any
series across the 2016→2021 boundary change. SA2 count grew 2,310 → 2,473; LGAs
change annually via amalgamations; 2016 LGA names keep the `(C)`/`(S)`/`(A)` type
suffix that 2021 dropped.

## Sources

- **2021 (Edition 3)** allocation files, `.xlsx`, one per structure —
  `abs.gov.au/.../edition-3-july-2021-june-2026/access-and-downloads/allocation-files`
  (aggregate structures = one row/unit; LGA/POA/SAL/CED/SED = mesh-block level).
- **Correspondences**, `.csv` — same site, `.../correspondences`
  (`CG_SA2_2016_SA2_2021.csv`, `CG_LGA_2016_LGA_2021.csv`).
- **2016 (Edition 2)** allocation files — ABS catalogues **1270.0.55.001** (Main
  Structure) and **1270.0.55.003** (Non-ABS). ABS ausstats serves HTML
  interstitials, not files; downloaded from the Internet Archive mirrors
  (`archive.org/download/1270055001-asgs-2016-07`,
  `archive.org/download/1270055003-2016-07`). Latin-1 encoded. 2016 suburbs are
  **SSC** (renamed SAL in 2021); CED 2016 is SA1-grain with no state column
  (state derived from the SA1 code); MB and LGA ship per-state (concatenated).

## Rebuild

```bash
cd models/br_bd_diretorios_au/code
python architecture/gen_architecture.py     # 23 architecture CSVs (source of truth)
python gen_dbt.py                            # 23 dbt models + schema.yml
# download 2021 allocation/correspondence files to input/2021, input/correspondences
# download 2016 files to input/2016/csv (see Sources)
python clean.py                              # -> output/<table>.parquet (one row/unit)
python upload.py                             # -> basedosdados-dev.br_bd_diretorios_au_staging
cd ../../.. && uv run dbt run --select br_bd_diretorios_au && uv run dbt test --select br_bd_diretorios_au
```

`clean.py` reads the architecture CSVs and maps source→target by each column's
`original_name`; parent integrity is enforced by dbt `relationships` tests, and
each PK by a `[diretorio]`-tagged uniqueness test. `input/` and `output/` are
gitignored (never commit data).
