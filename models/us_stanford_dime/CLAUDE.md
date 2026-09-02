# us_stanford_dime — DIME v4.0

Database on Ideology, Money in Politics, and Elections, compiled by Adam Bonica
and published by Stanford Libraries at <https://data.stanford.edu/dime>.

- **Licence:** ODC-BY 1.0 — share, create and adapt with attribution. No
  non-commercial clause, so nothing here is gated.
- **Citation:** Bonica, Adam. 2024. *Database on Ideology, Money in Politics,
  and Elections: Public version 4.0* [Computer file]. Stanford, CA: Stanford
  University Libraries.
- **Coverage:** the 1980 to 2024 election cycles.
- **Cadence:** static. Version 4.0 was released in December 2024 and DIME ships
  roughly every two years, so there is no recurring Prefect pipeline.

## Tables

| Table | Grain | Rows |
|---|---|---|
| `contribution` | one itemized contribution | ~861M |
| `recipient` | candidate or committee × cycle | 910,156 |
| `contributor` | one resolved donor | ~36M |
| `contributor_cycle` | donor × cycle they gave in | reshaped from wide |
| `dicionario` | code → label | 158 |

`recipient` comes from `dime_recipients_all`, not `dime_recipients`: it is the
same 65 columns plus `included_in_scaling`, so the smaller file is recoverable
by filtering and nothing is lost by taking the superset.

`contributor_cycle` exists because the source contributor file is wide, with
23 `amount.YYYY` columns that are mostly zero. Only non-zero donor-cycle pairs
are stored.

## What the source calls a cycle

The partition column is `cycle`, not `year`. A DIME cycle is the two-year
election cycle named for its even end year, so a contribution dated
2023-11-22 belongs to cycle 2024. Naming it `year` would misstate what it
means; the row's own calendar date is in `date`.

## Layout

```
code/
├── architecture.py            source of truth: names, types, order, English text
├── i18n.py                    Portuguese and Spanish for every description
├── observations_i18n.py       ditto for the observations field
├── sheets.py                  the Drive architecture sheet per table
├── constants.py               source URLs and the codebook's row counts
├── clean.py                   CSV → all-STRING Parquet, plus the UTF-8 repair
├── upload.py                  streaming GCS upload + BigQuery staging load
├── run_backfill.py            download → clean → upload, one cycle at a time
├── verify_clean.py            per-column non-null diff, raw vs cleaned
├── gen_dbt.py                 renders the .sql models and schema.yml
├── gen_dicionario.py          builds the dictionary table
└── gen_architecture_sheets.py renders the trilingual rows for the Drive sheets
```

Everything downstream is generated from `architecture.py`. Change a column
there and re-run `architecture.py`, `gen_dbt.py` and
`gen_architecture_sheets.py` rather than editing the outputs.

## Things that bit, and why the code looks the way it does

**The contribution table does not fit on disk.** Its Parquet is roughly 82 GB
and the machine converting it had ~60 GB free. `upload.py::stream_cycle` runs
DuckDB's `COPY ... FILE_SIZE_BYTES` and ships each completed part to GCS as it
appears, deleting it locally, so peak usage is the source file plus a couple of
parts. Part *n* is finished as soon as part *n+1* exists; the last is shipped
after the process exits.

**The conversion is a subprocess, not a thread.** The first version ran the
COPY on a worker thread while the main thread polled for finished parts. The
poller held the GIL often enough to stall the conversion outright — a part file
sat at zero bytes for six minutes while the poller burned 93% of a core. It
also spun instead of sleeping when a part was present but unreadable. Both are
fixed, but the subprocess split is what makes the stall impossible.

**Codebook row counts are line counts.** The codebook lists 447,970 rows for
1980; the file has 447,962 records and 447,970 physical lines, the difference
being the header plus 7 newlines inside quoted fields. Python's `csv` module
and DuckDB agree on 447,962. Treat the codebook figure as a ceiling.

**A few source lines are not valid UTF-8.** One line in the 1990 cycle carries
0xE3 0x9D inside a city name, and 2006 has its own. DuckDB refuses the whole
file, and its `latin-1` mode rejects the same bytes. `clean.sanitize_source`
repairs the file with `iconv -c`, which drops only the invalid sequences, and
`run_backfill` retries once. The data is otherwise almost pure ASCII — 0
non-ASCII bytes in the whole 1996 file, 16 in 2010 — so nothing legitimate is
lost.

**`ignore_errors` is deliberately off.** It would silently drop malformed rows,
and a quiet loss across 861M records is worse than a loud failure. Row counts
are checked against the codebook instead.

**Only `\N` means missing.** Scanning whole CSV fields shows the source uses
`\N` and the empty string and nothing else; `NA`, `NULL`, `.` and `-` never
appear alone. Treating those as null would destroy real values.

**Expensive tests are scoped.** `not_null_proportion_multiple_columns` compiles
a scan over every column and `unique_combination_of_columns` is a full shuffle;
unscoped on 861M rows they would burn a large slice of the daily BigQuery
quota. Both are pinned to `cycle = 2024` on `contribution`. A literal is honest
here because the dataset is static.

**`icpsr_id` is the recipient key.** The codebook says so and it verifies: zero
duplicates across 910,156 rows. `(cycle, recipient_id)` does not work — 11% of
rows duplicate it, since a recipient can contest more than one seat in a cycle.

## Deliberate non-links

`contributor_state` and `recipient_state` are two-letter abbreviations that
include territories and foreign codes, and the state directory's key is the
FIPS `id_state`, so there is no clean foreign key. `census_tract_id` follows
whichever decennial boundaries were in force for the cycle, so it does not
match a single-vintage tract directory. `contributor_district` moves with
redistricting for the same reason. All three are documented on the column
rather than linked.

## Not included

The CRP/NIMSP itemized records — 27,352,201 rows — are excluded from the public
DIME download and are licensed CC BY-NC-SA, unlike the rest. They are available
from Bonica by request for academic use only, and are out of scope here.

The office-grouped files (`contribDB_president`, `_governor`, `_judicial`) are
subsets of the cycle files and add nothing. The SQLite build, the `.rdata`
files and the sparse contingency matrix are alternative encodings of the same
data.
