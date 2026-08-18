# au_ato_taxation_statistics — onboarding notes

Source: ATO "Taxation Statistics" collection on data.gov.au
(organization `australiantaxationoffice`), one CKAN package per financial
year. Licence: **CC BY 2.5 AU** (`license_id: cc-by-2.5`,
`http://creativecommons.org/licenses/by/2.5/au/`), registered as the generic
`cc_by`, matching `au_ato_abr`.

## What was onboarded

Five long fact tables plus a dicionario, from the 2016-17 to 2023-24
releases (eight releases):

| Table | Source sheet | Grain | Years | Rows |
|---|---|---|---|---|
| `individuals_income_state` | Individuals Table 4 | sex × taxable status × state × taxable income range | 2016-2023 | 627,646 |
| `individuals_industry` | Individuals Table 5 | sex × state × broad industry | 2016-2023 | 306,286 |
| `individuals_postcode` | Individuals Table 6A | taxable status × state × SA4 × postcode | 2016-2023 | 3,350,614 |
| `company_industry` | Company Table 4A | broad × fine industry | 2016-2023 | 171,999 |
| `gst_industry` | GST Table 4 | broad × fine industry | 2017-2024 | 8,720 |
| `dicionario` | derived | — | — | 564 |

Total 4,465,829 rows. `year` is the **start year of the financial year**:
2023 denotes 2023-24.

## The shape, and why it is long

Every ATO detailed table is a few dimension columns followed by measure
columns that come in `<item> no.` / `<item> $` pairs — 80 to 110 pairs per
table. Melting those pairs into `item` / `record_count` / `amount` gives one
schema that absorbs the year-to-year churn in the item set: 267 distinct
items appear across the eight releases, and new ones simply arrive as new
`item` values instead of new columns.

## Source traps handled

1. **Table numbers are not stable.** The ATO reused `gst4` for petroleum
   resource rent tax before 2014-15 and for the by-industry table after, and
   switched from `company4` to `company04` in 2014-15. Resources are
   therefore selected on the number *plus* the descriptive filename slug.
2. **The release year is not the data year.** The GST by-industry table
   shipped in the 2023-24 release covers the **2024-25** financial year. The
   year is parsed from each sheet's own title, never from the package.
3. **Footnote digits are glued to labels** (`Net rent4`,
   `Statistical Area Level 4 (SA4)2`, the value `Other2`). Stripped only when
   attached to a letter or a closing bracket, so `Other income category 1`,
   `Subtotal 2` and `Tax loss 2022-23 carried back to 2021-22` survive.
4. **A/B sheet splits are different grains, not continuations.** Company 4B
   is finer than 4A but carries only 7 measures before 2018-19, so 4A is
   used. Individuals 6B is 6A totalled over taxable status, so taking both
   would double-count; 6A is used.
5. **`sa4_name` only exists from the 2021-22 release** and is null for
   earlier years — expected, not a gap in the transform.

## Deferred, and why

Scoped deliberately to individuals, companies and GST. Everything below is a
clean follow-up, not a blocker.

- **Earlier releases (2009-10 to 2015-16).** 2009-10 and 2010-11 hold only
  overview documents. 2011-12 to 2015-16 use the older `taxstats<YYYY>`
  naming, split the postcode table into 06a/06b/06c, and ship duplicate CSV
  copies — each needs a hand-verified resource pin. GST by-industry does not
  exist before 2014-15 at all.
- **The other ~90 tables per release**: superannuation funds, partnerships,
  trusts, CGT, FBT, excise, PAYG, cost of compliance, charities, and the
  individuals tables beyond 4/5/6 (occupation, deductions, percentiles,
  rental property, super contributions).
- **The "by year" headline tables** (Individuals/Company/GST Table 1), which
  are items-as-rows × years-as-columns — a different pivot. Largely redundant
  with the stacked panel here.
- **Company Table 4B**, the finer ATO industry-code grain with business
  descriptions, once its pre-2018-19 shape is handled.
- **Geography linking beyond state.** `postcode` carries ATO residual
  groupings (`NSW other`, `Overseas`, `Unknown`) and `sa4_name` is a name
  rather than a code, so neither is linked to `br_bd_diretorios_au`.
- **Step 12, the recurring annual pipeline.** The transform already lives in
  `pipelines/datasets/au_ato_taxation_statistics/` for exactly this.

## Tier

Annual release, so **AllFree** — the BD Pro rolling window applies to tables
refreshed monthly or more often. All coverages are `is_closed=False`.
