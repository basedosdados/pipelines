# us_fec_campaign_finance — onboarding plan

**Source:** Federal Election Commission bulk data, <https://www.fec.gov/data/browse-data/?tab=bulk-data>
**Organization:** FEC (Federal Election Commission) — new organization, slug `fec`
**License:** `cc0`. Works of the US federal government are not subject to copyright
(17 U.S.C. §105), so FEC bulk data is public domain. Same treatment as `us_cfpb_hmda`
and `us_fed_fred`.
**Area:** `us` **Theme:** `politics`
**Coverage:** election cycles 1980–2026 (2004–2026 for `disbursement`, 2000–2026 for
`candidate_committee_link`)

Transaction-level records of who funds US federal campaigns and what campaigns spend
it on — the canonical source for the money side of American political economy. Donor
identity, employer and occupation are itemized, so the data supports work on
contributor networks, industry influence, incumbency advantage and independent
expenditure. That research value is why the full 1980–2026 panel is loaded rather
than recent cycles only.

## Source mechanics

The FEC publishes one ZIP per file type per **two-year election cycle**, labelled by
the even year in which the cycle ends, at

```
https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/<CYCLE>/<prefix><YY>.zip
```

Each ZIP holds a single pipe-delimited text file with **no header row**. Files are
republished daily during a cycle; past cycles are frozen.

**No API key is required.** The OpenFEC REST API needs a free key, but every field in
this design comes from the bulk flat files, which are unauthenticated. The dataset
therefore handles no credential at all — a deliberate simplification over the
API-based alternative.

| Table | Prefix | Member | Fields | First cycle |
|---|---|---|---|---|
| `candidate` | `cn` | `cn.txt` | 15 | 1980 |
| `committee` | `cm` | `cm.txt` | 15 | 1980 |
| `candidate_committee_link` | `ccl` | `ccl.txt` | 7 | 2000 |
| `contribution_individual` | `indiv` | `itcont.txt` | 21 | 1980 |
| `contribution_committee` | `pas2` | `itpas2.txt` | 22 | 1980 |
| `committee_transaction` | `oth` | `itoth.txt` | 21 | 1980 |
| `disbursement` | `oppexp` | `oppexp.txt` | 25 (+1) | 2004 |

Total compressed download: **22.1 GB**, of which `indiv` is 20.5 GB and the 2020
cycle alone is 5.6 GB.

### Parsing quirks, all verified against the files rather than the docs

1. **`oppexp` has a trailing delimiter** — every line carries 26 fields for 25
   documented columns. The 26th is empty and is dropped.
2. **Two date formats.** `oppexp` uses `MM/DD/YYYY`; `indiv`, `pas2` and `oth` use
   `MMDDYYYY`. Both are normalized to ISO `YYYY-MM-DD` so `safe_cast(x as date)`
   resolves.
3. **Files are unquoted and contain stray double quotes** inside names. They must be
   parsed with `QUOTE_NONE`, or a single `"` swallows the rest of the file.
4. **Encoding is latin-1**, not UTF-8.

## Architecture

`code/architecture/*.csv` is the source of truth for column names, order and types.
`clean.py`, `gen_dbt.py` and the metadata step all read it; regenerate with
`code/architecture/build_architecture.py` rather than editing the CSVs.

- **English column names**, because the data and its documentation are English.
- **Partition column `cycle` (INT64)** — the two-year election cycle. Deliberately
  *not* linked to `br_bd_diretorios_data_tempo.ano`: a cycle is a two-year period,
  not a year.
- **Types follow arithmetic meaning.** Only `transaction_amount` (USD) and the year
  columns are numeric. `sub_id`, `file_number`, `line_number`, `office_district` and
  every FEC code look numeric but are identifiers or labels, so they are STRING.
- **Counterparty columns are named for their role**: `contributor_*` on the two
  contribution tables, `counterparty_*` on `committee_transaction` (which carries
  transfers in *and* out), `payee_*` on `disbursement`.
- **`has_sensitive_data = yes`** on the contributor name, city, ZIP, employer and
  occupation of `contribution_individual`. The records are statutorily public, but
  they are still individual-level personal data.
- **`directory_column` only on `candidate.office_state`**, which the FEC controls.
  Filer-entered address states include foreign, military (AA/AE/AP) and typo values,
  so they are left unlinked and documented in `observations` instead of carrying a
  foreign key that does not hold.

### Observation levels

Follows the `br_tse_eleicoes` precedent (`candidatos` → person/year/election_type):

| Table | Observation levels |
|---|---|
| `candidate` | `person` (candidate_id), `year` (cycle) |
| `committee` | `committee` (committee_id), `year` (cycle) |
| `candidate_committee_link` | `person`, `committee`, `year` |
| `contribution_individual` | `donation` (sub_id), `committee`, `year` |
| `contribution_committee` | `donation` (sub_id), `committee`, `year` |
| `committee_transaction` | `transaction` (sub_id), `committee`, `year` |
| `disbursement` | `expenditure` (sub_id), `committee`, `year` |

## The dictionary, and what is deliberately not tested

`custom_dictionary_coverage` fails on *any* unmapped value. Measuring actual coverage
(`code/audit_codes.py`) separates two very different cases:

**Genuinely closed code sets — tested at 100%:** `office`,
`incumbent_challenger_status`, `committee_designation`, `committee_type`,
`filing_frequency`, `amendment_indicator`, `transaction_type`, `entity_type`,
`report_type`, `candidate_status`, `organization_type`, `memo_code`.

Reaching 100% on these required two honest additions to the dictionary:

- **Legacy report types** `10D`/`10G`/`10P`/`10R`/`10S` and `24` — real FEC codes
  from pre-2000 filings that the current code-description page omits. ~46,000 rows.
- **`UNDOCUMENTED` entries** for a handful of values in otherwise-closed fields that
  appear in no FEC list (`candidate_status` A/I/Q, 3 rows; `organization_type` H/I,
  46 rows; `entity_type` B/C/I, 23 rows; `memo_code` Y/H/M/0, 121 rows). They are
  labelled "Code not documented by the Federal Election Commission" rather than
  invented, which keeps the test strict.

**Filer-entered fields — excluded from the test, and why:**

| Column | Unmapped rows | Distinct unmapped | Examples |
|---|---|---|---|
| `candidate.party` | 0.28% | 136 | `GOP`, `Rep`, `UND`, state codes |
| `committee.party` | 0.70% | 137 | `NAT`, `GRN`, `dem`, `R` |
| `disbursement.category` | 0.90% | 199 | `TX`, `CA`, `0.6`, `---` |

These keep `covered_by_dictionary = yes` — the dictionary does explain >99% of values
and is where a reader should look. What would be wrong is gating CI on 100% closure
of a field the FEC does not validate on filing: the only way to pass would be to
invent dictionary entries for `---` and `0.6`, which is worse than not testing.

## BD Pro

The source republishes daily and the pipeline re-pulls the current cycle, so the
house rule applies: tables refreshed monthly or more often paywall their most recent
window.

| Table | Tier |
|---|---|
| `contribution_individual`, `contribution_committee`, `committee_transaction`, `disbursement` | `PartBdpro(free_lag=6 months)` on `transaction_date` |
| `candidate`, `committee`, `candidate_committee_link`, `dicionario` | `AllFree` |

The four PartBdpro tables need **both** a free and a pro Coverage registered before
the pipeline runs, or it hard-fails at `assert_coverage_topology`.

## Recurring pipeline

Cycle-based: re-pull the **current** cycle only and overwrite its partition; past
cycles are frozen and never re-downloaded. Poll compares the source against the
registered `Update.latest` before doing any work.

Because dictionary-coverage tests reference the sibling `dicionario` model, the flow
must **run every table, then test every table** — never interleave run/test per
table, which fails in a clean environment when a test reads a sibling that has not
been built yet.

## Scratch data

`~/Downloads/us_fec_campaign_finance_data/` (`input/`, `output/`), overridable with
`FEC_DATA_DIR`. Never in the repo or under Dropbox. Each cycle's ZIP is deleted right
after it is parsed, so peak disk stays near one archive rather than the full 22 GB.
Delete the whole tree at step 14.

## Deliberately out of scope

- **Candidate and committee financial summaries** (`weball`, `webl`, `webk`,
  `committee_summary`) — aggregates derivable from the transaction tables.
- **Electioneering communications, communication costs, independent expenditure
  24/48-hour reports, lobbyist bundling** — separate CSV files with their own
  layouts; a natural second phase.
- **The OpenFEC REST API** — adds a credential without adding data this design needs.
