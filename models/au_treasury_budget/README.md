# au_treasury_budget

Australian Commonwealth Budget aggregates and long-run projections, from the
Australian Treasury. Four tables, 16,382 rows.

| Table | Rows | Coverage | Source |
|---|---|---|---|
| `aggregate` | 15,900 | 1970-71 to 2029-30, over 12 release vintages | BP1 historical-data Statement, FBO Appendix B |
| `payment_growth` | 101 | 8 releases, 2022-23 to 2026-27 | BP1 Statement 3, Chart 3.8 |
| `igr_projection` | 328 | 2022-23 to 2062-63, 7 scenarios | 2023 IGR, Appendices A1 and A4 |
| `dicionario` | 53 | — | authored, coverage derived from the data |

Licence: **CC BY 4.0** — confirmed on `budget.gov.au/legal-notices.htm` and
`treasury.gov.au/copyright`, excepting the Commonwealth Coat of Arms and logos.
Attribution: © Commonwealth of Australia.

## The vintage dimension is the point

Every Budget and every Final Budget Outcome republishes the **whole** series back
to 1970-71 on its own basis. An earlier release is never overwritten by a later
one: each was correct as at its own date, and they disagree. Net debt for
2025-26, by the release that projected it:

| Release | $m | % GDP |
|---|---|---|
| 2022-23 March Budget | 864,653 | 33.1 |
| 2022-23 October Budget | 766,787 | 28.5 |
| 2024-25 Budget | 615,478 | 21.5 |
| 2025-26 Budget | 620,345 | 21.5 |
| 2026-27 Budget | 555,999 | 18.8 |

Keeping only the latest release would destroy that. `aggregate` keeps all twelve,
and `payment_growth` goes further: its source chart plots two vintages *in its
own columns*, so `source_release_id` and `series_release_id` differ on about half
its rows.

`estimate_type` separates outcomes from projections, and is read off the `(e)`
marker Treasury prints in the source table rather than inferred from the release
date. Final Budget Outcome releases carry outcomes only, as they should.

## Why DOCX

The aggregates are not published as `.xlsx` or `.csv` anywhere. They are
published as Word, in Budget Paper No. 1's historical-data Statement and in the
identical Appendix B of the Final Budget Outcome — the same eleven tables under
two names. A DOCX table is XML, so extraction is exact and checkable; this is not
transcription from a PDF chart, and no number in this dataset comes from one.

Four details of Word's XML cost real data if ignored, all found here rather than
anticipated, and all documented in `code/docx_tables.py`: non-breaking hyphens
are elements (so `1970-71` extracts as `197071`), merged cells shorten a row,
header spans and data spans must expand *differently* (repeating a spanned data
cell duplicated `8,290` across a 54-year series), and captions live in
paragraphs, with their footnote markers and unit suffix split into separate ones.

## Checks that actually prove something

Parsing cleanly proves nothing: a mislabelled column parses perfectly and yields
a plausible number. Each table is therefore checked against something the source
independently asserts, and `clean.py` refuses to write if any check fails.

| Check | What it proves | Result |
|---|---|---|
| Total receipts = taxation + non-taxation, every year of every release | The column labelling is right | **970 checks, 0 failures** |
| A release re-plotting another's growth rates must agree with it | Series columns are attributed to the right vintage | **38 checks, 0 failures** |
| IGR Appendix A4's baseline must reproduce Appendix A1's | The scenario columns are read in the right order | **28 checks, 0 failures** |

Beyond that: an unknown column heading raises rather than being dropped, an
unknown payment program raises, a coded value with no dictionary entry raises,
and two historical tables disagreeing about the same measure raises.

## What is excluded, and why

- **The 2015 Intergenerational Report** — PDF only. No chart data, no Word
  bundle. Excluded entirely.
- **The 2021 Intergenerational Report's tables** — that edition published chart
  data but no Word bundle, so its projection and sensitivity tables exist only
  inside the PDF.
- **The 2023-24 and 2020-21 Budgets' statistical tables** — those two releases
  published BP1 as a single PDF. Ten filename patterns across four statement
  numbers were probed; no per-statement DOCX exists. Their financial years still
  appear through the neighbouring FBO and Budget vintages.
- **MYEFO historical aggregates** — MYEFO does not republish the series. Its only
  spreadsheet, Appendix C Annex A, is payments to the states. MYEFO appears here
  only through `payment_growth`.
- **The chart-data corpus** — 11 releases × ~775 sheets. Modelled only where the
  unit is unambiguous; the archives ship whole in the auxiliary-file bundles.

## Two corrections to the brief

- **The payment growth rates are nominal, not real.** Chart 3.8's note reads
  "Shows major payments that are growing faster than *nominal* GDP over the
  projection period", and nothing in the chart is deflated. Recorded as
  `growth_basis = nominal`.
- **`ndis_medium_term_only` uses a different projection window** from every other
  program — 2029-30 to 2036-37 in the 2026-27 Budget against 2026-27 to 2036-37
  for the rest. The window is stated per series in the statement prose, not in
  the spreadsheet, which is why the windows are declared in
  `clean_payment_growth.PERIOD_OVERRIDES` rather than derived blindly.

## Refresh: static, by hand

**Recommendation: no recurring pipeline.** Treasury publishes three times a year
(Budget in March–May, MYEFO in December, FBO in September), and every release
moves something a poller would have to guess:

- the historical statement alternates between **Statement 10 and Statement 11**;
- the file is `bp1_bs-11.docx` in some years and `bp1_bs11.docx` in others;
- the chart archive is `chartdata.zip`, `chart-data-final.zip` or
  `budget_2022-23_chart_data.zip` depending on the year;
- the payment-growth chart is sheet `3.8`, `3.08`, `C3.08`, `C3.10`, `3.4` or
  `C3.6`;
- the projection window exists only in prose.

Worse, **budget.gov.au answers an unknown path with HTTP 200** and a 7,349-byte
"page not found" body, so a filename-keyed poller would not fail loudly — it
would save a web page as a `.docx` and report an empty statement. Every download
here is magic-byte checked for exactly that reason.

A refresh is: add the release to `code/releases.py`, run `download.py`,
`clean.py`, `upload.py`, `dbt run`/`dbt test`. Roughly an hour, three times a
year, and the parser's assertions catch a changed layout rather than absorbing
it. **The 2025-26 Final Budget Outcome is due around late September 2026** — the
page is currently a placeholder reading `download/.pdf | XXXKB` — and is the
first test of that path.

## Layout

```
code/
  releases.py            the release manifest: what exists, where, under what name
  download.py            fetch everything, magic-byte checked
  docx_tables.py         Word table reader (no third-party dependency)
  measures.py            column heading -> measure, sector, unit
  clean_aggregate.py     the 12-vintage historical series
  clean_payment_growth.py  Chart 3.8, both vintages per chart
  clean_igr_projection.py  IGR 2023 Appendices A1 and A4
  dictionary.py          definitions authored, coverage derived
  clean.py               runs all four, writes all-STRING staging parquet
  upload.py              -> basedosdados-dev staging, row counts verified
  columns.py             trilingual column descriptions
  metadata.py            backend registration, idempotent
  auxiliary_files.py     per-table bundles (see below)
  architecture/*.csv     the source of truth for names, types and units
```

## Known gaps

- **`auxiliary_files_url` is unset.** The bundles are built by
  `code/auxiliary_files.py` but not uploaded: the local service account has no
  `storage.objects.create` on `basedosdados` or `basedosdados-public`, and the
  documented bucket is requester-pays, so every already-published
  `auxiliaryFilesUrl` returns HTTP 400 to an anonymous visitor. Measured again
  2026-09-11. Registering a URL that does not resolve would be worse than
  leaving the field empty.
- **No `aud` measurement unit exists in the backend** (`brl`, `usd`, `cad`, `clp`
  and others do). The two dollar columns therefore carry an empty
  `measurement_unit`, with the currency named in the column name and description.
  Adding `aud` is a one-row reference fix that would improve several Australian
  datasets at once.
- **The organization slug differs by environment.** Staging already has
  `treasury` — it is the *United States* Department of the Treasury, area `us` —
  so the Australian one is registered as `au_treasury`, matching `au_abs`,
  `au_ato` and `au_doe` there. Production uses short slugs (`abs`, `ato`, `doe`),
  and has no Australian Treasury at all, so the prod promotion has to pick
  between `treasury` and `au_treasury`.
