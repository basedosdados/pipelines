# us_fbi_cde

The FBI's Uniform Crime Reporting program, published through the Crime Data
Explorer: incident-level NIBRS records from 1991, the Return A summary series
from 1985, agency employment from 1960, and the hate crime series from 1991.

## The coverage break is the first thing to know

**NIBRS coverage is partial and is not comparable across the 2021 transition.**
The FBI retired the Summary Reporting System as its primary collection in 2021.
NIBRS participation grows from three states in 1991 to every state in 2020, and
several of the largest agencies — the NYPD and the LAPD among them — filed
nothing in the 2021 transition year. A raw national sum from `incident` or
`offense` is therefore not a crime count and is not comparable year to year.

Every table that needs it carries the fields for a user to weight their own
coverage:

- `agency.population` — resident population of the jurisdiction, per year
- `agency.nibrs_months_reported` — 0 to 12, from the NIBRS monthly table
- `agency.summary_months_reported` — the highest month filed on the Return A
- `agency.covered_by_ori` — when set, this agency's crimes are counted under
  another agency, and summing both double counts

`ucr_summary` is the series that crosses the break: the Return A form is still
collected from agencies that never migrated, so it runs 1985 to 2025 without a
gap. It is the right table for a long national series; the NIBRS tables are the
right ones for incident detail.

## Licence

Works of the United States Government, not subject to copyright protection in
the United States (17 U.S.C. § 105). The FBI publishes no explicit licence
statement for the CDE. Registered as `cc0`, the house convention for US federal
public-domain data, matching `us_bea`, `us_bls_cpi` and `us_census_cps`.

## How the source is fetched

The download page never exposes a static object URL. It asks
`https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=<key>` for a presigned S3 URL
one object at a time; the URL expires after 900 seconds. No API key is needed.
A key that does not exist is simply absent from the response, which doubles as
an existence check — that is how the state-year availability matrix is built.

Object keys used:

| what | key |
|---|---|
| NIBRS bundle | `nibrs/incident/<year>/<ST>-<year>.zip` |
| Return A year | `master_files/reta/reta-<year>.zip` |
| employees | `additional-datasets/law-enforcement/lee_1960_2025.csv` |
| hate crime | `additional-datasets/hate-crime/hate_crime.zip` |
| documentation | `nibrs/_all/NIBRS_DataDictionary.pdf` |

## Two source shapes, and what they cost

**NIBRS bundles** are fifth-normal-form: 1,066 zips of ~43 normalised CSVs each,
13.9 GB compressed. They changed shape at the 2019/2020 redesign — the older
ones use lowercase filenames, ship `cde_agencies.csv` instead of `agencies.csv`,
key offences on a surrogate `offense_type_id` rather than `offense_code`, and
carry no `data_year` column at all. `clean_nibrs_bundle` normalises both.

**Return A** is a fixed-width flat file, LRECL 7,385, whose layout the FBI ships
only as a 15-page scanned PDF with no text layer. The layout was recovered by
OCR and is arithmetically self-consistent: a 305-character header plus twelve
590-character month blocks is exactly 7,385. It was then validated against the
data — every offence total equals the exact sum of its components, and the 2023
national figures match the FBI's published estimates. Negative adjustment
entries use the zoned-decimal overpunch (`}` = -0 through `R` = -9).

The 1990–1996 archives use PKWARE Implode, which Python's `zipfile` cannot
decompress; those fall back to the `unzip` binary.

## Design decisions

**Surrogate ids are resolved to published codes.** The FBI's own bundles key
everything on integers (`location_id`, `race_id`, `weapon_id`) that mean nothing
outside one download. Cleaning replaces each with the FBI's published code
(`location_code` `20`, `race_code` `W`) using the lookup CSVs inside the same
bundle. The code-to-label mapping is the `dicionario` table, harvested across
every bundle era so codes retired in the 1990s still have labels.

**Four link tables are folded, two are not.** Weapon, bias motivation, victim
injury and suspected drug are "up to N per parent" and are folded onto their
parent as the lowest-sorting code plus a count, so a user can see when the fold
lost detail. `victim_offense` and `victim_offender_relationship` are kept as
tables: the FBI's own README warns that an incident's offences must not be
attributed to all of its victims, so that mapping is load-bearing for
correctness.

**Group B arrests have no agency.** The published files carry
`nibrs_arrestee_groupb` with no incident and no ORI, only the state and year of
the file it came from. They are kept in `arrestee` with `arrest_group = 'B'` and
null `ori`/`incident_id` rather than dropped, since excluding them would
materially understate arrests.

**County FIPS is matched, not published.** The CDE publishes county names only.
`agency.county_id` is matched against the Census ANSI county list and is null
where the name has no unique match.

## Tables

| table | grain | partition | coverage |
|---|---|---|---|
| `agency` | ORI × year | `year` | 1960–2025 |
| `incident` | incident × year | `year`, `state_abbr` | 1991–2025 |
| `offense` | offence | `year`, `state_abbr` | 1991–2025 |
| `offender` | offender | `year`, `state_abbr` | 1991–2025 |
| `victim` | victim | `year`, `state_abbr` | 1991–2025 |
| `victim_offense` | victim × offence | `year`, `state_abbr` | 1991–2025 |
| `victim_offender_relationship` | victim × offender | `year`, `state_abbr` | 1991–2025 |
| `arrestee` | arrestee, Group A and B | `year`, `state_abbr` | 1991–2025 |
| `property` | property description | `year`, `state_abbr` | 1991–2025 |
| `hate_crime` | incident × year | `year` | 1991–2025 |
| `ucr_summary` | ORI × year × month × offence line item | `year` | 1985–2025 |
| `dicionario` | table × column × code | none | — |

## Refresh cadence

`23 15 2,6,10,14,18,22,26,30 9,10,11 *` — every four days across September to
November, when the FBI publishes the new data year. The source poll reads the
NIBRS `maxYear` from the CDE's own download catalogue and makes every run before
the release a no-op.

Each run refreshes the newest data year plus the two before it, because the FBI
revises those in the same release. Every table is year-partitioned and each
year's content depends only on that year's sources, so a windowed refresh is
complete for the years it touches.

`dump_mode` is `"append"` everywhere, never `"overwrite"`: overwrite calls
`tb.delete(mode="all")`, which drops the materialised production table even from
a dev-only run.

No table is `PartBdpro`. The BD Pro rolling window applies to tables refreshed
monthly or more often; this one refreshes annually.

## Renaming a partition key leaves the old one behind

`write_partition` writes into `year=<Y>/state_abbr=<ST>/`. It does not remove a
directory that a previous run used under a different key. When Nebraska was
remapped from the postal `NE` to the UCR `NB`, the earlier run's
`state_abbr=NE/` directories survived and the state was counted twice in all
eight NIBRS tables — no error, just wrong totals.

`pass_nibrs` now reports any partition on disk that the run did not write. It
reports rather than deletes, because the unexpected partition is sometimes the
new work. Clear the stale ones before validating.

The staging upload is not exposed to this: `upload.py` calls
`Storage.delete_table(mode="staging")` before every upload, which clears the
whole prefix.

## Rebuilding from scratch

```bash
python models/us_fbi_cde/code/build_architecture.py     # architecture CSVs + style checks
python models/us_fbi_cde/code/build_dicionario.py       # dicionario.csv from the bundles
python models/us_fbi_cde/code/build_dbt.py              # SQL models + schema.yml
python models/us_fbi_cde/code/clean.py --workers 3      # ~14 GB in, partitioned parquet out
uv run python models/us_fbi_cde/code/upload.py          # staging (dev) only
```

Scratch data lives in `~/Downloads/us_fbi_cde_data/`, never in the repo or
Dropbox. Override with `US_FBI_CDE_DATA`.
