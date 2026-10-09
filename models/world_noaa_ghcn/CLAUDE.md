# world_noaa_ghcn — Global Historical Climatology Network daily

NOAA NCEI GHCN-Daily: daily land-surface observations from ~132,500 stations
worldwide. Source: https://www.ncei.noaa.gov/pub/data/ghcn/daily/

**Read `SCOPE_DECISION.md` first.** This dataset ships the **full** archive —
every element, every year, 3.19 bn rows. A cut to the core five from 1950 was
drafted and rejected because neither candidate cut was actually a size cut
(15% and 19% respectively); the reasoning is not re-derivable from the code.

## Tables

| Table | Rows | Grain |
|---|---|---|
| `station` | 132,501 | one per station |
| `station_element_inventory` | 782,552 | station x element, first/last year |
| `observation` | 3.19 bn | station x date x element, all 144 elements, 1763+ |
| `dicionario` | 358 | code -> label for every coded column |

## Traps this source sets

1. **Scaling is per element.** PRCP is tenths of a millimetre, TMAX/TMIN are
   tenths of a degree C, but **SNOW and SNWD are already whole millimetres**.
   A blanket divide-by-ten silently divides snowfall by ten. The divisors live
   in `code/constants.py::ELEMENT_UNITS`, one entry per element.

2. **A non-blank quality flag means the value FAILED QC.** Rows are kept, not
   dropped. Verified on 2024: 100% of physically impossible values (TMAX above
   60 C, negative snow depth, 56 m snow depth) carry `quality_flag = 'X'`.
   Users must filter `quality_flag IS NULL`.

3. **`by_year/` file mtimes are useless as a change detector.** NCEI
   reconstructs the whole archive weekly, so all 264 files carry the same
   timestamp (observed 2026-09-07 19:28-19:30 across every file, 1763 to 2026).
   A refresh pipeline must not diff on mtime — see the pipeline notes below.

4. **`value` has no single unit, and for 28 elements no unit at all.** Units
   are carried per row in `measurement_unit`; the column-level metadata on
   `value` is deliberately blank. `FMTM`/`PGTM` hold an HHMM clock time and
   every `WT**`/`WV**` is an occurrence indicator whose value is always 1 —
   these carry a **null** `measurement_unit` and are listed in
   `constants.NON_QUANTITY_ELEMENTS` (1.14% of 2024 rows).

6. **An unmapped element code raises.** `clean_year` fails loud on any element
   absent from `constants.ELEMENT_UNITS`, because an unmapped code would take a
   null divisor and silently null out every one of its values. GHCN adds
   elements between versions — map the new code, do not drop it.

5. **Do not confuse this with nClimGrid.** Prod already holds a metadata shell
   `gridded_5km_ghcn_daily_temperature_and_precipitation_dataset_nclimgrid`
   (org `noaa`, zero tables). That is the *gridded* 5 km CONUS monthly product
   derived from GHCN-D — a different dataset. It was left untouched.

## Layout

```
code/constants.py         element units/divisors, flag code tables
code/clean.py             pure transforms, shared with the refresh pipeline
code/gen_architecture.py  writes code/architecture/*.csv (source of truth)
code/gen_dbt.py           writes the .sql models from the architecture
code/gen_schema.py        writes schema.yml from the architecture
code/year_row_counts.csv  measured rows per year, all 264 source files
```

Scratch data lives in `~/Downloads/world_noaa_ghcn_data/`, never in the repo.
