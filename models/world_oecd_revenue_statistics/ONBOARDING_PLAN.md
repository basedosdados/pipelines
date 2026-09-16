# world_oecd_revenue_statistics — onboarding notes

**Source.** OECD Global Revenue Statistics Database, via the SDMX REST API. Single
comparative dataflow `OECD.CTP.TPS,DSD_REV_COMP_GLOBAL@DF_RSGLOBAL,2.1`. Codes-only
CSV (`format=csvfile`). 146 economies (142 ISO3 countries + 4 OECD aggregates:
`OECD_REP`, `A9`, `F`, `SO`), 1990–2024, annual.

**Licence.** OECD Terms & Conditions §3 (reuse incl. commercial, attribution) →
`cc_by_igo`, BD-Pro compatible.

## Shape — WIDE, one table `revenue`

Grain: **country × year × government level × tax category**. The OECD unit
dimension is pivoted into five value columns rather than kept long, because the
five units are the same quantity expressed five ways:

- `pct_gdp` (PT_B1GQ), `pct_institutional_sector` (PT_OTR_SECTOR), `pct_revenue_category`
  (PT_OTR_REV_CAT) — percentages;
- `value_national_currency` (XDC) and `value_usd` (USD) — absolute, with the OECD
  `UNIT_MULT` applied; `currency` names the national-currency unit.

Dropped from the source: `MEASURE` (constant `TAX_REV` — non-tax revenue is carried
inside the tax-category tree, `T_NONTAX_*`), `CTRY_SPECIFIC_REVENUE` (always `_T`).
A runtime assertion in the transform fails loudly if either ever stops being
constant (which would collide the wide grain).

## The double-counting guard

The OECD tax classification is hierarchical (`_T` → `T_1000` → `T_1100` → `T_1110`…).
A naive sum over categories double-counts every parent. The table makes the
hierarchy explicit:

- `tax_category_id` — the `T_` code; `tax_category_parent_id` — its immediate parent
  (NULL for the top-level totals `_T`, `T_CUS`, `T_XSSC`); `tax_category_code` — the
  numeric OECD code.
- `pct_revenue_category` gives each category's share of its parent directly.

The individual/corporate split of income taxes is shipped as published; the
unallocable portion is the OECD category `T_1300` (noted in the column
`observations`), left for the user to allocate.

## Country handling

`reference_area` keeps the raw OECD area code (countries and aggregates);
`country_iso3_code` is the ISO3 code for the 142 real countries (FK to
`br_bd_diretorios_mundo.pais:sigla_iso3`) and NULL for the 4 aggregates, which have
no ISO3 — so the relationships test skips them.

## Layout

- `pipelines/datasets/world_oecd_revenue_statistics/{constants,utils}.py` — the pure
  transform (download, DSD/hierarchy parse, wide pivot, all-STRING partition write,
  dictionary derived from observed codes), shared with the recurring pipeline.
- `code/` — bootstrap: `download.py`, `clean_data.py`, `build_columns_json.py`,
  `upload.py`, `register_metadata.py`; `architecture/*.csv` (source of truth);
  `country_iso3.csv`.
- dbt: `world_oecd_revenue_statistics__{revenue,dicionario}.sql`, `schema.yml`.

## Consolidation note

This dataset repurposes the empty prod shell
`oecd_revenue_statistics_in_latin_america_and_the_caribbean` (id
`b0164aa7-1cbe-455c-8776-23de5022f4e8`) — the LAC regional subset, subsumed by the
global database. The other empty shell, `oecd_tax_database`
(`1d51d43c-99c3-4861-b69c-31fb54d76b8d`), is a **different** OECD product (statutory
and effective tax *rates*, VAT, environmental taxes) and is left untouched.

## Refresh

Annual. Last OECD update 2026-06-29 (ContentConstraint `validFrom`). A light
recurring Prefect pipeline is a follow-up (step 12).
