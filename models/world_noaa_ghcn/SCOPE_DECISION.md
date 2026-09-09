# world_noaa_ghcn — scope decision (SCALE GATE)

Decided 2026-09-09, before any build work. Measured, not estimated: every figure
below comes from streaming all 264 `by_year/*.csv.gz` files and counting rows.

## What the full archive actually is

| Quantity | Measured |
|---|---|
| `by_year/` files | 264 (1763–2026) |
| Compressed size | 14.13 GB |
| Total observation rows | **3,193,292,359 (3.19 bn)** |
| Rows in the 5 README "core" elements | **2,703,017,389 (2.70 bn = 84.6%)** |
| Distinct elements | 32 (1900) → 74 (2024); ~100 across the archive |
| Stations (`ghcnd-stations.txt`) | 132,501 |
| Inventory rows (`ghcnd-inventory.txt`) | 782,552 |
| Core-5 rows failing QC (non-blank Q-flag) | 6,013,106 (0.222%) |

## The candidate cuts, measured

The brief offered three cuts. Two of them turn out **not to be size cuts**:

| Cut | Rows kept | Saving |
|---|---|---|
| (a) core elements only, all years | 2.70 bn | **15%** |
| (b) 1950 onward, all elements | 2.60 bn | **19%** |
| (a) + (b) together | **2.16 bn** | **32%** |
| (c) GHCN-Monthly v4 instead | — | different product |

Element filtering saves only 15% because the core elements *are* the archive —
PRCP alone is 30% of 2024 rows. Year filtering saves only 19% because the
pre-1950 record is thin (1900 = 4.6M rows against 2024's 37.1M).

## Decision: (a) + (b) — the five core elements, 1950 onward

`TMAX, TMIN, PRCP, SNOW, SNWD` for `1950-01-01` onward. **2.16 billion rows.**

Three reasons, in order of weight:

1. **(a) is a correctness cut, not a size cut.** The agreed architecture is one
   `value FLOAT64` column with an explicit `measurement_unit`. That is only
   honest for a unit-coherent element set. Across all ~100 elements the units
   span degrees C, mm, hPa×10, degrees of arc, percent, minutes, cm, HHMM clock
   times, and boolean weather-type occurrence flags — `WT03` ("thunder
   occurred") and `PGTM` ("peak gust at 14:32") cannot share a numeric column
   with a stated unit. Restricting to the five core elements is what makes the
   specified architecture correct. It is worth doing even at zero size saving.

2. **(b) keeps the build inside proven scale.** The largest table this repo has
   shipped is `us_fbi_cde` at 1.32 bn rows. 2.16 bn is 1.6× that; 2.70 bn is
   2.0×, and 3.19 bn is 2.4×. 1950 is also the conventional modern-record cut for
   climatology, so it needs no bespoke justification to a user.

3. **(c) is rejected as non-responsive.** GHCN-Monthly v4 is monthly mean
   temperature — a different product that does not deliver daily precipitation,
   snow, or station-day observations. It is not a first phase of this dataset;
   it is a different dataset.

## What is deliberately deferred (phase 2, documented not silent)

- **Pre-1950 core-5**: 0.55 bn rows, back to 1763. Disproportionate scientific
  value per row; the only reason it is not in phase 1 is build risk.
- **The ~95 non-core elements**: 0.49 bn rows. Needs a different table shape —
  they are not unit-coherent and should not be appended to `observation`.
  `TAVG` (5.3% of 2024 rows, tenths °C) and `TOBS` (4.2%) are the natural first
  additions and *are* unit-coherent with TMAX/TMIN.
- The `station_element_inventory` table ships the **full** element list for all
  years, so a user can see exactly what phase 1 omits.

## Two design consequences worth flagging

- **`value` carries two units.** Even within the core five, TMAX/TMIN are °C and
  PRCP/SNOW/SNWD are mm. A single column-level `measurement_unit` cannot
  describe the column. Resolved by carrying a per-row `measurement_unit` STRING
  column and leaving the column-level metadata blank with an explicit note.
  A **wide** table (one row per station-day, five typed value columns) would
  avoid this *and* cut rows ~2.3× to ~930M — but the brief specified LONG, so
  LONG is what is built.
- **Scaling is per element, not global.** PRCP is tenths of mm but **SNOW and
  SNWD are already whole millimetres** in the source. Applying a blanket ÷10 to
  "precipitation-like" elements would silently divide snowfall by ten.

## Build constraint

Local disk has 64 GB free against ~40-50 GB of expected parquet. The backfill
therefore streams **one year at a time**: download → clean → parquet → upload →
delete, never materialising the whole output.
