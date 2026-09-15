# us_ffiec_bank_reporting

FFIEC regulatory filings: the quarterly Call Report schedules of every US bank,
the FR Y-9C consolidated financials of bank holding companies, and the annual
Community Reinvestment Act small business and small farm lending disclosure.

Backend slug `bank_reporting`, GCP dataset `us_ffiec_bank_reporting`,
organization `ffiec` (created by this onboarding), licence `cc0` (US government
work, 17 U.S.C. §105 — the CDR Legal Notice claims no copyright and imposes no
redistribution condition).

## Where this sits in the finance cluster

It deepens the cluster rather than duplicating it:

| Dataset | What it carries |
|---|---|
| `us_fdic_bankfind` | the FDIC's **curated summary** of bank financials |
| **this** | the **full regulatory schedules underneath** that summary |
| `us_cfpb_hmda` | mortgage lending, loan-application level |
| **this** (`cra_lending`) | the **small-business-credit counterpart** to HMDA |

`rssd_id` is the join key across every table here, and corresponds to
`us_fdic_bankfind.institution.cert`. **That correspondence was verified, not
assumed**: on a random sample of 200 banks, `institution.fdic_cert_id` →
`us_fdic_bankfind.institution.rssd_id` agreed 98/98 exactly, with no cert
missing from BankFind.

## Nine tables

| Table | Grain | Rows |
|---|---|---|
| `institution` | bank × quarter | ~430k |
| `call_report_item` | bank × quarter × MDRM item | ~580M |
| `holding_company` | holding company × quarter | ~490k |
| `holding_company_item` | holding company × quarter × MDRM item | ~200M |
| `mdrm_item` | MDRM item code | 75,264 |
| `cra_respondent` | institution × year | ~25k |
| `cra_lending` | institution × year × county × tract income group × band | ~100M |
| `cra_assessment_area_tract` | institution × year × census tract | ~70M |
| `dicionario` | coded column × key | 117 |

## Coverage, and why it stops where it does

- **Call Report 2009Q3–2026Q2.** The CDR advertises 102 periods back to 2001 but
  **30 of them return 0-byte zips**: every quarter before 2007, plus 2007Q2–Q4,
  2008Q2–Q3 and 2009Q2. 2007Q1, 2008Q1, 2008Q4 and 2009Q1 do have data but are
  isolated, so the series starts at the first gap-free quarter.
- **Holding company financials 1986Q3–2026Q2**, spliced across two hosts: the
  Chicago Fed archive (comma CSV) to 1999Q4, then FFIEC NPW (caret-delimited)
  from 2000Q1. The Chicago Fed archive stops at 2021Q1; NPW is used for
  everything from 2000 on.
- **CRA 1996–2024.**

## The things that will bite you

**`www.ffiec.gov` is behind a JS/cookie WAF.** `requests`, `curl` and `wget` all
get an HTTP 403 CAPTCHA page, for the HTML *and* for the zip files. `curl_cffi`
with `impersonate="chrome124"` is admitted — the same fix `us_dol_oflc` uses for
`www.dol.gov`, and `curl_cffi` is already a repo dependency. Byte-for-byte
identical to what a real browser gets. `cdr.ffiec.gov` is not affected.

**The CDR bulk download is an ASP.NET WebForms postback.** You cannot GET a file.
Select the product, let the postback populate the period dropdown, carry
`__VIEWSTATE` through a cookie jar, then submit. See `download.py::_cdr_session`.

**Dollar amounts are filed in THOUSANDS and counts are not.** MDRM's `ItemType`
lumps both under `F`, so the unit is classified in `mdrm.py::classify_unit` and
only `USD` items are multiplied by 1,000. Getting this wrong scales a value by
1,000 in silence. It is checked against reality, not trusted: `validate.py`
compares total assets against the published `us_fdic_bankfind` figures, and the
smoke test reproduces JPMorgan at $4.091T and Wells Fargo at $1.908T for 2026Q2.

Two name-matching traps were hit while writing that classifier, both the same
shape as the `us_fdic_bankfind` "ratio fired inside Operations" bug:

- matching `YEAR`/`MONTH` anywhere in a name stripped USD from **273 genuine
  dollar amounts** — Call Report lines are routinely named "DEBT SECURITIES WITH
  A REMAINING MATURITY OF OVER THREE YEARS".
- matching a trailing `DATE` caught "...DURING THE CALENDAR YEAR-TO-DATE".

Only anchored rules survive. The classifier now leaves 27 Call Report items with
no unit, and all 27 are genuinely yes/no questions, an audit code or a date.

**Filer counts swing tenfold between quarters, and that is correct.** The BHCF
files bundle two forms: FR Y-9C, the quarterly consolidated report (item codes
`BHCK`, `BHCA`, `BHDM`…), filed by ~350–450 large holding companies; and FR
Y-9SP, the **semiannual** small-parent report (`BHSP`), filed by ~3,700 more in
June and December only. So Q2 and Q4 files carry ~4,200 rows and Q1 and Q3 ~420.
Verified on 2020Q2: 350 filers carry `BHCK` items, 3,740 carry `BHSP`. Filter on
the `item_code` prefix, or join `mdrm_item.reporting_form`, to separate them. An
early read of this as a truncated Chicago Fed file was wrong.

**Percentage values carry a `%` suffix** (`"20.1508%"`). Without stripping it,
every capital ratio in the dataset is dropped as unparseable.

**Some items are booleans typed as financial.** `RCONP752` and `RIAD4769` hold
`true`/`false`, and their MDRM names give nothing away ("OTHER EXPLANATIONS").
They are recognised from the **value**, not the name, and kept out of the FLOAT64
fact tables. `CONF` marks a figure the FFIEC suppresses and is treated as NULL.

**7% of item codes are filed on more than one schedule.** `RCON2170` (total
assets) appears on both RC and RC-R Part II with the same value. Left alone that
makes `(rssd_id, item_code)` a non-key and double-counts those items in any sum.
The first schedule alphabetically owns the code; agreement across schedules is
**checked** per quarter, not assumed, and a disagreement is logged as a warning.

**The CRA flat files changed layout twice, and the current spec matches nothing
before 2004.** Three eras, each with its own record width:

| Years | Table id | MSA/MD | count / amount | Lending record |
|---|---|---|---|---|
| 1996 | 4 chars | 4 | 6 / 8 | 113 |
| 1997–2003 | 5 chars | 4 | 6 / 8 | 114 |
| 2004–2024 | 5 chars | 5 | 10 / 10 | 145 |

Parsing everything at the 2004+ offsets silently drops all of 1996–2003 — eight
of the twenty-nine years — because every record fails the length guard.
`clean.py::_cra_era` accumulates each era's offsets from field widths and checks
the total against the observed record length. The widths come from the per-year
"File Specifications" PDF, whose own printed start/end columns contain
off-by-one typos; the widths are consistent, the offsets are not.

Two more era differences: files are **one combined `.dat` before 2016** and eight
separate ones after, and the **1996 transmittal has no `ID_RSSD` field at all**
(132 characters against 152), so 1996 CRA rows carry a NULL `rssd_id` by
necessity and cannot be joined to the Call Report. Every later year links 100%.

**Read the era's spec, not the current one — twice over.** The 2024 file
specification lists three CRA agency codes (1=OCC, 2=FRS, 3=FDIC) because the
Office of Thrift Supervision was abolished by Dodd-Frank in 2011. The 1997-2010
specs list **4=OTS**, and it is on 3,133 respondents — 11% of the CRA rows —
from 1996 to 2010, disappearing after. Decoding from the latest spec alone
leaves all of them carrying a bare `4`. Same failure mode as parsing 1996
records at 2004 offsets: the current spec is not the spec for the whole series.

Related design point: every coded field falls through to the **raw source code**
when the map has no entry, rather than to NULL. That is what made this visible
as a stray `4` in a `COUNT(DISTINCT agency_id)` instead of 3,133 silent NULLs.

**`NA` and blank are different values in the CRA files, and collapsing them
corrupts the table.** For the assessment area number, `NA` is lending *outside*
any assessment area while blank is the *total across all of them*; same shape for
MSA/MD. Running both through the usual null-token map made the row key
non-unique and double-counted each total against its own components. Verified on
respondent 324 in county 42017: `0001` (1 loan, $250k) + `NA` (1 loan, $200k) =
blank (2 loans, $450k). `_cra_field` keeps `NA`; after the fix the declared key
has zero duplicates across every year.

**The CRA respondent id is not an RSSD.** It is assigned by the supervising
agency — an OCC charter, an FRS RSSD or an FDIC certificate depending on
`agency_code`. Only the transmittal sheet carries `ID_RSSD`, which is why
`cra_respondent` exists and is the bridge to every other table.

**CRA lending has no per-tract amounts.** The D1/D2 disclosure files are
aggregated to county × tract-income-group × report level. The only tract-grain
file, D6, is assessment-area *membership* with no dollars — that is
`cra_assessment_area_tract`. `census_tract_id` carries **no directory foreign
key**: the series spans four decennial tract vintages and the source does not
restate earlier years, so a single-vintage FK would be wrong for most of it.

## Layout

```
code/
  common.py            paths, coverage constants, the numeric-item rule
  mdrm.py              MDRM loader and the unit classifier
  dictionary.py        value -> label maps for the coded columns
  schema_def.py        THE source of truth for all 110 columns
  translations.py      PT/ES for all 84 distinct descriptions
  download.py          all four sources, resumable
  clean.py             melt + partition to all-STRING parquet
  build_architecture.py / build_trilingual.py / build_dbt.py
  upload.py            -> basedosdados-dev staging only
  validate.py          six checks, incl. units against us_fdic_bankfind
  meta_config.py / register_metadata.py
```

Scratch data lives at `~/Downloads/us_ffiec_bank_reporting_data/`
(`FFIEC_DATA_DIR` overrides). Never in the repo, never in Dropbox.

## Known follow-ups

- `RCON9224`, the bank's Legal Entity Identifier, is filed on a Call Report
  schedule but is alphanumeric, so it is excluded from the numeric fact table and
  logged. It would be a genuinely useful addition to `institution`.
- `holding_company.charter_type_id`, `organization_type_id` carry NIC code lists
  that are not published with the data; the values are carried as filed rather
  than guessed at.
