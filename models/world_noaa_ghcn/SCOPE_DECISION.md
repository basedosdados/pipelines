# world_noaa_ghcn — scope decision (SCALE GATE)

Decided 2026-09-09, before any build work. Measured, not estimated: every figure
below comes from streaming all 264 `by_year/*.csv.gz` files and counting rows.

## Decision: load the full archive

**Every element, every year. 3,193,292,359 observation rows, 1763 to 2026.**

An initial cut to the five core elements from 1950 was drafted and then
**rejected on the evidence below**, which is kept because it is the reason the
full load is defensible rather than merely ambitious.

## What the full archive actually is

| Quantity | Measured |
|---|---|
| `by_year/` files | 264 (1763–2026) |
| Compressed size | 14.13 GB |
| Total observation rows | **3,193,292,359 (3.19 bn)** |
| Rows in the 5 README "core" elements | 2,703,017,389 (2.70 bn = 84.6%) |
| Distinct elements | 144 across the archive; 74 in 2024 alone |
| Elements whose value is not a quantity | 28 (1.14% of 2024 rows) |
| Stations (`ghcnd-stations.txt`) | 132,501 |
| Inventory rows (`ghcnd-inventory.txt`) | 782,552 |
| Core-5 rows failing QC (non-blank Q-flag) | 6,013,106 (0.222%) |

Per-year counts are committed at `code/year_row_counts.csv`.

## Why the candidate cuts were rejected

| Cut | Rows kept | Saving |
|---|---|---|
| (a) core elements only, all years | 2.70 bn | **15%** |
| (b) 1950 onward, all elements | 2.60 bn | **19%** |
| (a) + (b) together | 2.16 bn | **32%** |
| (c) GHCN-Monthly v4 instead | — | different product |

**Neither candidate cut is a size cut.** Element filtering saves 15% because
the core elements *are* the archive — PRCP alone is 30% of 2024 rows. Year
filtering saves 19% because the pre-1950 record is thin (1900 = 4.6M rows
against 2024's 37.1M). Paying a third of the data to avoid a third of the cost
is a bad trade when the third being dropped includes the 19th-century series
that make GHCN distinctive.

(c) was rejected as non-responsive: GHCN-Monthly v4 is monthly mean
temperature, a different product that carries no daily precipitation or snow.

## The one real objection, and how it is handled

Element filtering was originally proposed on **correctness**, not size: the
agreed architecture is one `value FLOAT64` column with an explicit
`measurement_unit`, and that is dishonest across 144 elements whose units span
degrees C, mm, hPa, degrees of arc, percent, minutes, cm, km, m/s, days, HHMM
clock times and boolean occurrence indicators.

That is fixed rather than avoided:

- `measurement_unit` is carried **per row**, not per column. The column-level
  metadata on `value` is deliberately blank, with the reason recorded in the
  column's `observations`.
- The **28 elements whose value is not a measurable quantity** — `FMTM` and
  `PGTM` (a clock time in HHMM) and every `WT**` / `WV**` (an occurrence
  indicator whose value is always 1) — carry a **null** `measurement_unit`.
  They are listed in `constants.NON_QUANTITY_ELEMENTS`, and the clean step
  asserts that the null-unit set in the output matches that list exactly.

Verified on 2024: 422,494 rows (1.14%) have a null `measurement_unit`, and the
set of elements carrying one matches `NON_QUANTITY_ELEMENTS` with zero
disagreements.

## Residual risk, stated plainly

3.19 bn rows is ~2.4x the largest table this repo has shipped (`us_fbi_cde`,
1.32 bn). The untested step is prod materialisation via the table-approve
action, which has previously OOM'd on large staging parquet. That risk is not
created by the full load — it applied to the 2.16 bn variant too — but the full
load makes it ~48% larger.

## Build constraints

- Local disk has ~62 GB free against ~60-75 GB of expected parquet, so the
  backfill **streams one year at a time**: download, clean, upload, delete.
  The whole output is never materialised locally.
- An element code absent from `constants.ELEMENT_UNITS` raises rather than
  silently producing a null value. GHCN adds elements between versions, and an
  unmapped code would otherwise null out every one of its values.

## Two source traps that survive into the pipeline

- **Scaling is per element, not global.** PRCP is tenths of a millimetre but
  **SNOW and SNWD are already whole millimetres**. A blanket divide-by-ten
  across "precipitation-like" elements silently divides snowfall by ten.
- **`by_year` file mtimes cannot detect change.** NCEI reconstructs the whole
  archive weekly, so all 264 files carry one identical timestamp (observed
  2026-09-07 19:28–19:30 on every file from 1763 to 2026). The refresh pipeline
  must diff on size or content, never on modification date.
