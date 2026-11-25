# au_abs_population — onboarding notes

ABS population estimates and projections, from three source products:

| Source | Former catalogue | Cadence | Release slug seen |
|---|---|---|---|
| National, state and territory population | 3101.0 | quarterly | `dec-2025` |
| Regional population | 3218.0 | annual | `2024-25` |
| Population projections, Australia | 3222.0 | irregular (re-based every few years) | `2022-base-2071` |

Licence: **CC BY 4.0**, the ABS standard, stated on every release page.

## Tables

| Table | Grain | Rows | Span |
|---|---|---|---|
| `national_state` | quarter x region x sex x measure | 15,563 | 1981 Q2 - 2025 Q4 |
| `erp_age_sex` | year x region x sex x single year of age | 149,985 | 1971 - 2025 |
| `projection` | year x series x region x sex x age | 355,050 | 2022 - 2071 |
| `regional_sa2` | year x SA2 | 61,335 | 2001 - 2025 |
| `regional_lga` | year x LGA | 13,700 | 2001 - 2025 |
| `series` | ABS Series ID | 9,921 | — |

The brief suggested three tables. Six were built; each split is forced by a
measured property of the data, not by taste:

* **`national_state` / `erp_age_sex`** — 3101.0 ships two different things. The
  quarterly workbooks are region x measure flows; tables 51-59 are an *annual*
  series on a region x sex x single-year-of-age grain. Different frequency and
  different grain, so different tables. `erp_age_sex` deliberately matches
  `projection` column for column, so the actual and projected population can be
  unioned or compared directly.
* **`regional_sa2` / `regional_lga`** — LGAs do not nest inside SA2s and resolve
  to a different directory, so one table cannot carry both with a valid foreign
  key.
* **`series`** — the dimension table the brief left optional. Taken, because the
  ABS Series ID is stable across ABS releases and products and is the natural
  join key to other ABS output.

### Why the ASGS hierarchy is one table, not five

3218.0 publishes ERP at SA2, SA3, SA4, GCCSA and state. Every published
aggregate was checked against the sum of its SA2s, at every level, in every
year: **zero mismatches**. The coarser levels are therefore exactly recomputable
by grouping `regional_sa2` on `sa3_id` / `sa4_id` / `gccsa_id` / `state_id`,
which the table carries as directory-linked columns, so they are not stored
again. SUA, Remoteness Area, CED and SED are out of scope: two of them have no
directory, and the brief scoped the work to SA2, LGA and GCCSA.

## ASGS vintage — the thing that had to be right

The 2024-25 release publishes the **whole 2001-2025 back-series on ASGS Edition
3 (2021) boundaries**. Verified: all 2,454 SA2 codes match `sa2_2021` with 0
unmatched, and 303 of them are absent from `sa2_2016`. So there is **no 2016/2021
break inside this table** — the break is against figures published in earlier
releases, and `br_bd_diretorios_au.correspondence_sa2_2016_2021` bridges it.

LGAs are the exception. ABS restates onto boundaries newer than the ASGS 2021
LGA directory, so three codes have no 2021 entry and are ignored in the
relationship test, documented in the model description:

| LGA | Why |
|---|---|
| 24700 Merri-bek | renamed from Moreland, 2022 |
| 71500 East Arnhem | East Arnhem Regional Council split, 2023 |
| 71700 Groote Archipelago | the other half of that split |

## Traps found and handled

1. **Two description orders.** 3222.0 state workbooks read
   `measure ; series ; region ; sex ; age`; the Australia workbook reads
   `measure ; sex ; age ; series`. Parsing by position silently mislabels every
   national row, so each part is classified by shape instead.
2. **Mixed units inside one workbook.** 3101.0 Table 1 publishes Australia in
   thousands, Tables 2/4/16 publish the same quantities in persons, and the
   percentage-change series are in percent. The fact table therefore carries a
   `unit` column, and where a measure appeared in both, the persons figure is
   kept — all 895 collisions were verified to differ by at most 50 persons,
   exactly half of the 0.1-thousand rounding unit. After deduplication every
   measure resolves to exactly one unit.
3. **Density cannot be derived.** ABS computes population density from the
   *unrounded* area while publishing area rounded to 0.1 km2. Recomputing
   `erp / area_sqkm` reproduced only 35% of SA2 densities within 0.1 and erred
   by up to 2,769 persons/km2 on small dense areas. The published value is
   carried for the one year ABS publishes it, and never recomputed; it sits at
   4% non-null and is excluded from the non-null proportion test.
4. **Sentinels.** The data cubes use `..` for not-applicable, which must become
   NULL rather than a literal string.
5. **Footnote rows.** Cube sheets append footnote and copyright lines below the
   data; rows are kept only when the code cell matches the level's code shape.

## Reproducing

```bash
python models/au_abs_population/code/clean_data.py --download
bash   models/au_abs_population/code/dump_directory_ids.sh /tmp
python models/au_abs_population/code/validate.py ~/Downloads/au_abs_population_data/output /tmp
python models/au_abs_population/code/upload.py
uv run dbt run  --select models/au_abs_population
uv run dbt test --select models/au_abs_population
```

`gen_architecture.py` and `gen_dbt.py` regenerate the architecture CSVs and the
dbt models/schema from one spec; the architecture is asserted against
`constants.COLUMNS` so the three cannot drift.

The download and cleaning transform lives in
`pipelines/datasets/au_abs_population/utils.py` and is shared with the recurring
Prefect pipeline — it is not duplicated here.

## Recurring pipeline

`au_abs_population_flow` rebuilds all six tables and full-replaces them
(`dump_mode="overwrite"`), because every ABS release ships the full history.
Two polls, because two of the three sources move on different cadences: the run
proceeds when **either** the quarterly 3101.0 series or the annual 3218.0 series
has advanced. Each poll's `date_format` matches its table's coverage
granularity — a year compared against a month-granular coverage never fires.
Projections ride along and are simply re-replaced unchanged between re-basings.

Scratch data lives in `~/Downloads/au_abs_population_data` and is deleted once
the onboarding is verified.
