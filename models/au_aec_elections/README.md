# au_aec_elections

Australian Electoral Commission federal election results and political finance
disclosures.

Two sources, one dataset:

- **Results** — [results.aec.gov.au](https://results.aec.gov.au/), the AEC Tally Room
  archive. Per-event bulk CSV covering the House of Representatives and the Senate,
  down to individual polling places.
- **Transparency Register** —
  [transparency.aec.gov.au](https://transparency.aec.gov.au/), the annual, election and
  referendum financial disclosure returns.

Licence: **CC BY 4.0** (Commonwealth of Australia). Confirmed at
`aec.gov.au/footer/copyright.htm`. The Commonwealth Coat of Arms, AEC logos and AEC
maps are excluded from the licence; none of them are in this dataset.

## Coverage

| Event type | Events | Years |
|---|---|---|
| General elections | 8 | 2004, 2007, 2010, 2013, 2016, 2019, 2022, 2025 |
| By-elections | 24 | 2008–2026 |
| Senate-only election | 1 | 2014 Western Australia |
| Referendum | 1 | 2023 |
| Annual disclosure returns | — | financial years 1998-99 to 2024-25 |
| Election disclosure returns | 12 events | 1996–2025 |

**The two halves do not span the same years.** The results archive starts at 2004; the
Transparency Register's election returns start at the 1996 federal election. So
`disclosure_election_return` carries returns for the 1996, 1998 and 2001 elections that
have no counterpart in any results table.

**Two events are deliberately absent.** The 2001 federal election (AEC event 10822) and
the 2005 Werriwa by-election (event 12426) publish no CSV downloads at all — only HTML
result pages. They are excluded rather than scraped.

**By-elections publish less than general elections.** The AEC releases only four files
per by-election: candidates, first preferences by polling place, two candidate preferred
by polling place, and two party preferred by polling place. By-elections therefore appear
in the polling-place tables and in `house_candidate`, but not in the division-level
tables (`house_first_preference_division`, `house_two_party_preferred_division`) or in
any Senate table.

## Tables

| Table | Grain | Rows |
|---|---|---|
| `election` | event | 34 |
| `polling_place` | event × polling place | 79,840 |
| `party` | event × state × party registration | 589 |
| `house_candidate` | event × candidate | 8,818 |
| `house_first_preference_division` | event × division × candidate | 9,763 |
| `house_first_preference_polling_place` | event × polling place × candidate | 584,719 |
| `house_two_candidate_preferred_polling_place` | event × polling place × candidate | 141,152 |
| `house_two_party_preferred_division` | event × division | 1,202 |
| `house_two_party_preferred_polling_place` | event × polling place | 70,576 |
| `senate_candidate` | event × candidate | 3,492 |
| `senate_first_preference_division` | event × division × candidate | 123,442 |
| `division_summary` | event × chamber × division | 2,570 |
| `referendum_polling_place` | event × question × polling place | 8,735 |
| `disclosure_donation` | donation | 74,022 |
| `disclosure_receipt` | receipt | 124,282 |
| `disclosure_return_annual` | financial year × return | 20,977 |
| `disclosure_election_return` | event × return | 16,197 |
| `dicionario` | coded value | 84 |

Every table except `dicionario` is partitioned by `year`.

## Things worth knowing before using this

**`election_id` is the join key, not `year`.** Several events can share a year: 2018 had
seven by-elections, and 2014 had both the Griffith by-election and the Western Australia
Senate election. Join on `election_id`, and use `election` to interpret it.

**`division_id` is the AEC's identifier, not the ABS CED code.** It is stable within the
AEC's own systems but does not join to `br_bd_diretorios_au.commonwealth_electoral_division_*`,
which is keyed on ABS codes. Divisions are also redistributed between elections, so a
division id is only meaningful alongside its event. `state_abbreviation` does join to
`br_bd_diretorios_au.state`.

**`disclosure_donation` can double count.** The Transparency Register collects the same
donation from both ends: the donor reports what they gave, and the recipient reports what
they received. Both reports are carried, distinguished by `direction` (`made` /
`received`). Filter to one direction before summing, or you will count some donations
twice. The split is heavily skewed — 68,922 `made` against 5,100 `received` — because
donor returns are far more numerous than recipient itemisations.

**`sitting_member` exists only for 2004.** That election publishes `SittingMemberFl` and
no `Elected` / `HistoricElected`; every later election does the reverse. All three columns
are kept, and each is NULL for the vintages that do not publish it.

**Two-party-preferred column order flips between elections.** The AEC ships Labor first in
some years and the Coalition first in others. The cleaning transform selects by column
name, never by position, and `validate.py` checks the result against the known national
outcomes (Coalition ahead in 2013, Labor ahead in 2025).

**The AEC's own data has two malformed rows.** The 2023 referendum polling-place file
ships two rows with a surplus empty address field. They are repaired, not dropped, and
the repair is printed during cleaning.

**Third party electoral expenditure categories are not carried.** `Third Party Returns.csv`
breaks electoral expenditure into five categories; `disclosure_return_annual` carries the
total but not the breakdown. The source file is linked as a raw data source.

**No polling dates.** The bulk CSVs do not carry the date an election was held, so
`election` records the year but not the date.

## Code layout

The cleaning transform lives in `pipelines/datasets/au_aec_elections/` so that the
one-shot onboarding here and any future recurring pipeline share one implementation.

```
pipelines/datasets/au_aec_elections/
├── constants.py   # event catalogue, file lists, paths
├── schema.py      # column and table specification — the source of truth
└── utils.py       # pure download + cleaning functions

models/au_aec_elections/
├── code/
│   ├── architecture/          # generated from schema.py
│   ├── build_architecture.py  # schema.py -> architecture CSVs + columns.json
│   ├── build_dbt.py           # schema.py -> .sql models + schema.yml
│   ├── clean.py               # download + clean + write parquet
│   ├── upload.py              # parquet -> BigQuery staging
│   └── validate.py            # cleaned output vs. raw sources
├── au_aec_elections__*.sql
└── schema.yml
```

`schema.py` is the single source of truth. Change a column there, then re-run
`build_architecture.py` and `build_dbt.py`; do not hand-edit the architecture CSVs, the
`.sql` models or `schema.yml`.

## Reproducing

Raw downloads and cleaned parquet go to `~/Downloads/au_aec_elections_data/`
(override with `$AEC_DATA`) — never inside the repo or Dropbox.

```bash
uv run python models/au_aec_elections/code/clean.py          # download + clean (~85 MB in)
uv run python models/au_aec_elections/code/validate.py       # verify against sources
GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
  uv run python models/au_aec_elections/code/upload.py --env dev
uv run dbt run  --select au_aec_elections
uv run dbt test --select au_aec_elections
```

## Recurring updates

Results are episodic — a general election every ~3 years, by-elections in between. The
Transparency Register publishes annual returns on the first business day of February and
election returns 24 weeks after polling day. A recurring pipeline is therefore low value
between events; the download and cleaning functions in `utils.py` are already
Prefect-ready if one is added.
