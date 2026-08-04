# au_alexander_politicians — post-2021 update mechanism (scoping)

The source (Rohan Alexander, `RohanAlexander/australian_politicians`) was last
updated **29 November 2021** and is effectively frozen. This note scopes how the
dataset could be brought forward to the current Parliament and kept current. It
is a design, not an implementation.

## Why this is not a plain scheduled scrape

The dataset is a **hand-reconciled union** of many sources (Parliamentary
Handbook, Wikipedia, Australian Dictionary of Biography, Senate Biographies,
Trove, Wikidata). Two things carry the value and neither is published upstream:

1. The stable `id_politician` (surname + birth year, hand-disambiguated).
2. The qualitative `comments` / end-reason judgements.

There is no single feed to poll. So a refresh has to *reconstruct* new rows from
structured upstreams and *reconcile* them against the existing ids. That
reconciliation is the agentic part, and it needs a human gate.

## The two tractable upstreams

| Upstream | What it gives | Access |
|---|---|---|
| **Wikidata** | Every row already carries `id_wikidata`. SPARQL on "position held" (`P39` = member of the Australian House of Representatives `Q18912794` / Senator `Q19546127`) returns officeholders with start/end dates (`P580`/`P582`), party (`P102`), electorate (`P768`), and state — including the 2022 (47th) and 2025 (48th) intakes. | `https://query.wikidata.org/sparql`, no auth, CC0 |
| **AEC** | Authoritative elected-members lists per federal election (2022, 2025) and by-elections. Confirms names, divisions, states, party. | `results.aec.gov.au` downloads / `transparency.aec.gov.au`; CC BY 4.0 (see `project_australia_gov_data_audit`) |
| **APH Parliamentary Handbook** | `id_aph` already links each politician to `handbook.aph.gov.au`. Member/senator pages are structured and carry current membership + ministries. | `handbook.aph.gov.au`; Crown copyright — check reuse before redistribution |

Wikidata is the spine (it maps 1:1 to existing rows via `id_wikidata`); AEC is the
authoritative cross-check for House/Senate membership; APH backfills ministries.

## Proposed refresh flow (semi-automated, human-gated)

```
1. PULL      Wikidata SPARQL (MHR + Senator officeholders, all dates)
             AEC elected-members CSVs (2022, 2025, by-elections)
2. MATCH     join to existing politician by id_wikidata (exact)
             -> unmatched: fuzzy match on (surname, first_name, birth_year)
3. PROPOSE   agent drafts new/changed rows:
               - new politician rows (mint id_politician = surname+birthYear)
               - new party_affiliation / house_member / senator / ministry spells
               - closed date_end for members who left
4. REVIEW    *** human gate ***  every newly-minted id_politician and every
             fuzzy (non-id_wikidata) match is confirmed by a person
5. APPEND    accepted rows -> clean.py inputs -> re-run clean/upload/dbt
6. COVERAGE  extend datetime range end; register table Update
```

Steps 1–3 and 5–6 automate cleanly. Step 4 is irreducible: minting a duplicate
`id_politician`, or mis-matching two people who share a surname and birth year,
corrupts every downstream join. The gate is small (tens of rows per election),
so this is cheap to run once per federal election (~every 3 years) plus
by-elections, not a high-frequency cron.

## Effort / recommendation

- **Feasible, moderate effort.** The Wikidata SPARQL + AEC pull + id_wikidata
  join is a day of work; the fuzzy-match + review UI is the rest.
- **Cadence:** event-driven (after each federal election / by-election), not a
  fixed schedule. A Prefect flow could poll AEC for a new election result and
  open a review batch, but it should never append without the gate.
- **Not built here.** This onboarding lands the static 1901–2021 snapshot. Build
  the updater as a separate follow-up (`onboarding-pipeline`-style, but with the
  human-review gate) if/when bringing the data to the current Parliament is
  wanted.

## Open questions for the build

1. Redistribution terms for APH Handbook content (Crown copyright) — Wikidata
   (CC0) + AEC (CC BY 4.0) may suffice and avoid the APH question entirely.
2. Whether to preserve Alexander's exact `id_politician` convention for new
   entrants or adopt `id_wikidata` as the durable key going forward.
