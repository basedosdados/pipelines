# us_ssa_beneficiaries — onboarding notes

OASDI and SSI beneficiary counts and benefit amounts by US county and state,
from the Social Security Administration's Office of Retirement and Disability
Policy.

## Source

SSA publishes these statistics as per-year HTML and Excel tables, one page per
state per year. It **also** publishes *flattened time series* — JSON files
concatenating every annual edition — which is what this onboarding uses.

| Series | Files | Coverage | Landing page |
|---|---|---|---|
| OASDI Beneficiaries by State and County | `oasdi_state_county_table_{1..5}.json` | 1999– | [flat-series](https://www.ssa.gov/policy/docs/statcomps/oasdi_sc/flat-series.html) |
| SSI Recipients by State and County | `ssi_state_county_table_{1..3}.json` | 1998– | [flat-series](https://www.ssa.gov/policy/docs/statcomps/ssi_sc/flat-series.html) |

Each file carries its own schema metadata — dimensions, measures with label,
type and unit, sources, footnotes — so the cleaning transform is driven by the
published metadata rather than by column positions. The `.csv` links advertised
on the same pages are generated in the browser from the JSON and 404 server-side.

`www.ssa.gov` is behind Akamai and returns HTTP 403 to `requests`, `urllib` and
plain `curl` regardless of headers. `curl_cffi` with `impersonate="chrome124"`
is admitted.

Licence: work of the US federal government, public domain (registered as `cc0`).

## Tables

Five long fact tables plus a dictionary, 1,605,533 rows.

| Table | Rows | Years | Grain |
|---|---|---|---|
| `oasdi_county` | 956,912 | 1999–2025 | county × year × benefit type × age group × sex |
| `ssi_county` | 617,951 | 1998–2025 | county × year × eligibility category × age group × concurrent OASDI |
| `oasdi_state` | 17,358 | 1999–2025 | state or area × year × same three dimensions |
| `ssi_state` | 10,450 | 1998–2025 | state or area × year × same three dimensions |
| `oasdi_population_share` | 2,808 | 1999–2025 | state or area × year × population group |
| `dicionario` | 54 | — | — |

### Why three dimension columns rather than one

The eleven OASDI measures are two overlapping cuts of one universe: nine benefit
types that sum to the total, and a sex split of the 65-or-older subset. Folding
them into a single category column would make `sum(beneficiary_count)`
double-count silently. Splitting them into `benefit_type`, `age_group` and `sex`
keeps both cuts addressable and makes the hazard explicit — a sum is only
meaningful within one consistent slice. SSI mirrors this with
`eligibility_category`, `age_group` and `oasdi_concurrent`.

## Source characteristics handled in the transform

**Amounts are stored in dollars, converted from the published thousands.**
The annual table headers say "(in thousands of dollars)"; the JSON metadata
mislabels the unit as `dollars`. Dec 2024 OASDI is 68,455,973 beneficiaries
against a published total of 125,577,970, so the figures are thousands. Stored
as dollars, that total is $125,577,970,000, an average monthly benefit of
$1,834.43, which matches SSA's own published average. SSI works out at $744.63.

SSA has already rounded to the nearest thousand, so **every stored amount ends
in three zeros**: the conversion changes the unit, not the precision. That
caveat is on the column in all three languages.

**Suppression is carried, never zeroed.** SSA marks disclosure suppression with
`(X)` (39,773 cells in `ssi_county`) and "less than $500" with `a`. Both become
NULL with the reason preserved in a per-measure `*_note` column.

**County codes before 2008 are reconstructed.** SSA introduced ANSI codes in the
2008 (OASDI) and 2009 (SSI) editions and back-filled null for earlier years. The
codes are recovered from the same file's later years — SSA's own spelling, not an
external gazetteer. 99.99% of OASDI and 99.5% of SSI county rows resolve; what
remains NULL is genuinely unmappable (the "Unknown" rows, Alaska census areas and
Virginia independent cities abolished before codes were first published).

This is harder than it looks because the naming regime changed twice: before
2005 independent cities carry a `" City"` suffix and are interleaved
alphabetically with counties; from 2005 the suffix is dropped and the cities move
to a trailing block, but codes do not appear until 2008. In that 2005–2007 window
seven names — Baltimore, St. Louis, Bedford, Fairfax, Franklin, Richmond,
Roanoke — appear twice in the same state with only position to separate them, and
are resolved by occurrence. The result is checked for continuity across the
2007/2008 boundary, where a mis-assignment would show up as a jump in the series.

**The 2010 state counts are recovered.** SSA's Table 2 omits the entire 2010
edition, though Tables 3 and 4 have it. The missing numbers come from Table 4's
own "State total" rows, which are identical to Table 2 in every year where both
exist.

**County-grain tables exclude SSA's "State total" rows**, which belong to the
state tables. This keeps the grain clean; the state tables also cover territories
and the residual areas that the county tables do not.

## Reconciliation gate

A year is accepted only if it reconciles against SSA's own published totals:
county sums against each state total, and the sum over areas against the
published national total. The tolerances are asymmetric because the two
directions mean different things.

| Direction | OASDI | SSI | Cause |
|---|---|---|---|
| county sum > state total | +0.022% | never | rounding of county values |
| county sum < state total | −0.69% | −3.74% | rounding, plus suppression |
| sum over areas vs national | −0.0002% | −0.0002% | rounding only |

A shortfall is expected: a suppressed county contributes nothing to the sum, and
Hawaii has five counties of which two are suppressed. An excess has no benign
explanation, so that side is held tight. The gate runs inside `clean_all`, so a
mis-parsed year raises rather than reaching BigQuery.

## Auxiliary files

None. SSA publishes no codebook, questionnaire or import script for this
series: the JSON files carry their own schema metadata, and the explanatory
notes are part of that metadata rather than separate documents. Those notes are
captured in the column descriptions and `observations`. The annual editions are
also available as long-form PDFs, which belong in the link-only category and are
reachable from the raw data source URL.

## Known gaps

- **The SSI federal/state supplement split is not available from this series.**
  It lives in SSI Table 4, which SSA does not publish as a flat file because its
  structure varies across editions.
- **No `relationships` test on `county_id`.** Fifteen historically correct codes
  are absent from `br_bd_diretorios_us.county`, which holds only current
  counties: Connecticut's eight pre-2022 counties, five dissolved Alaska areas,
  Shannon SD (renamed Oglala Lakota in 2015) and Bedford city VA (reverted to a
  town in 2013).

## Rebuilding

```bash
uv run python models/us_ssa_beneficiaries/code/build_architecture.py
uv run python models/us_ssa_beneficiaries/code/build_dbt_models.py
uv run python models/us_ssa_beneficiaries/code/clean.py --download
uv run python models/us_ssa_beneficiaries/code/upload.py
uv run python models/us_ssa_beneficiaries/code/test_reconciliation.py
```

Scratch data goes to `~/Downloads/us_ssa_beneficiaries_data` (override with
`SSA_DATA_DIR`), never inside the repo or Dropbox. The cleaning transform lives
in `pipelines/datasets/us_ssa_beneficiaries/utils.py` and is shared with the
recurring Prefect flow, so the two cannot diverge.

## Metadata

One script registers everything, and is safe to re-run because every record is
looked up before it is written:

```bash
uv run python models/us_ssa_beneficiaries/code/metadata.py --env staging
uv run python models/us_ssa_beneficiaries/code/metadata.py --env prod
uv run python models/us_ssa_beneficiaries/code/metadata.py --env prod --publish
```

`--dataset-only` updates the dataset record alone, which is what a tag or
description change needs; walking all six tables takes about ten minutes.

Datasets are created `under_review`. Staging is published before the PR so a
reviewer sees the dataset as it will appear; prod is published only after the
PR merges, table-approve materialises the tables and they are verified.

Two things the script encodes that are easy to get wrong:

- **Tags are resolved by uuid, not slug.** The same tag record is
  `previdencia_social` on staging and `social_security` on prod. A slug list
  resolves to a different set per environment, or to nothing.
- **`get_dataset` costs about 25 seconds**, so it must not sit inside a loop.

Run exactly one instance at a time. Concurrent runs race on the
lookup-then-create step and can duplicate observation levels and coverages.
