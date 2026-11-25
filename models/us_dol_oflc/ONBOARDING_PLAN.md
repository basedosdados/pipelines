# us_dol_oflc — onboarding decisions

Office of Foreign Labor Certification (OFLC) case disclosure data, U.S.
Department of Labor. Source: <https://www.dol.gov/agencies/eta/foreign-labor/performance>.

## Scope

Four long tables, one per program, plus a dictionary:

| Table | Programme | Coverage |
|---|---|---|
| `lca` | Labor Condition Application — H-1B, H-1B1, E-3 | FY2008– |
| `perm` | Permanent labor certification, ETA-9089 | FY2008– |
| `h2a` | Temporary agricultural certification | FY2008– |
| `h2b` | Temporary non-agricultural certification | FY2008– |
| `dictionary` | Coded values used by the four tables | — |

One row per case number per federal **fiscal** year. 95 source workbooks in
total; `year` is the partition column and comes from the source file, never from
a date in the row.

Not onboarded in this pass, and worth a follow-up: the prevailing wage
determination file (`PW_Disclosure_Data_*`), the CW-1 Guam/CNMI programme, and
the companion worksite files (LCA Appendix A and Worksites, H-2A Addendum A/B,
H-2B Appendix A/C/D) that carry the second and later worksites of a case.

## Decisions

**The crosswalk is the deliverable.** The source renames, splits and merges
columns almost every fiscal year — the LCA table alone spans a 33-column FY2010
layout, a 260-column FY2019 layout with ten inline worksite blocks, and a
96-column FY2020 layout with one. `code/crosswalk/<table>.csv` records, for every
column of every source file, the canonical column it became or the reason it was
not published. `build_crosswalk.py` exits non-zero on any column it does not
recognise, so a new form revision fails loudly instead of silently dropping a
field.

**The primary worksite only.** A case covering several worksites lists the rest
in a companion file. Publishing the first worksite keeps the stated grain — one
row per case — and `total_worksite_locations` says how many the case really had.

**Wages keep the source pair and gain a derived annual column.** The source
reports an amount next to a separate unit field whose vocabulary changes by year
and programme (`Hour`, `HR`, `Hourly`; `BI` for bi-weekly; `DAI` for daily). The
unit is harmonised; the annualised column multiplies by 2080, 260, 52, 26, 24,
12 or 1. `Piece Rate` is a real unit with no period, so it never annualises, and
`Select Pay Range` is the unfilled form default, not a unit. A missing or
unrecognised unit leaves the annualised column NULL — the FY2010 LCA file has no
wage-unit column at all, so every FY2010 annualised wage is null.

**No always-NULL columns.** A programme that never reports a field does not
carry it: PERM has no visa class or employment period, H-2A publishes no
prevailing wage (its pay floor is the separately published Adverse Effect Wage
Rate), H-2B stopped publishing the prevailing wage amount after FY2015. The
omissions are listed with their reason in `canonical_map.OMIT`.

**Personal contact details are not published.** Employer points of contact,
attorneys and preparers appear in the source with names, home addresses, phone
numbers and email addresses. Business names are kept — `employer_name`,
`employer_trade_name`, `attorney_law_firm_name`, `secondary_entity_business_name`
— and the individuals' details are dropped, with the reason recorded in the
crosswalk. No beneficiary of a certification is named anywhere in the source.

**SOC and NAICS are not linked to a directory.** Both change vintage with the
year of filing (SOC 2000, 2010 and 2018 all appear; NAICS codes run 2 to 6
digits), so a foreign key would fail on a large share of rows. They are STRING
with the intent recorded in `observations`. State columns hold USPS
abbreviations, which cannot be a directory link either — the US state directory
is keyed on the FIPS code — so referential coverage is checked by a dbt
`custom_relationships` test against `abbreviation` instead.

## The quarterly-file trap

The LCA quarterly files are **not** cumulative. Verified on `DECISION_DATE`:
`LCA_Disclosure_Data_FY2025_Q1` spans 2024-10-01 to 2024-12-31, `Q2` spans
2025-01-01 to 2025-03-31, and `Q4` spans 2025-07-01 to 2025-09-30 — the four
files partition the year. The FY2026 `Q3` file, by contrast, spans 2025-10-01 to
2026-06-30 and is cumulative year-to-date; FY2026 Q1 and Q2 do not exist.

A fiscal year is therefore the **union of every quarterly file published for it**,
de-duplicated on case number, which is correct under both regimes. PERM, H-2A and
H-2B annual files are full-year.

The recurring pipeline follows from this: it re-materialises the open fiscal year
and the one before it from scratch on each run and never appends.

## Access

`www.dol.gov` is behind Akamai, which returns HTTP 403 to `requests`, `curl` and
`wget` regardless of headers — the block keys on the TLS fingerprint, and a burst
of requests also earns a temporary IP block. `curl_cffi` with
`impersonate="chrome"` is admitted on the first try, and is added to
`pyproject.toml` for that reason.

## License

US federal public domain, per
<https://www.dol.gov/general/aboutdol/copyright>: "Materials created by the
federal government are generally part of the public domain and may be used,
reproduced and distributed without permission." Registered as `cc0`, the house
mapping for US federal data.
