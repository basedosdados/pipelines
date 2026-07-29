# GERDA (German Election Database) — Onboarding Plan

**Status:** APPROVED (2026-07-28) — building
**Source:** https://www.german-elections.com/ · repo `awiedem/german_election_data` · R pkg `gerda`
**Paper:** Heddesheimer, Hilbig, Sichart & Wiedemann (2025), *GERDA: The German Election Database*, Nature Scientific Data 12:618. DOI 10.1038/s41597-025-04811-5
**License:** CC BY 4.0
**GCP dataset id:** `us_nature_gerda`  (bare slug `gerda`; org token `us_nature`)
**Organization (metadata):** Nature (`us_nature`) — resolve/confirm backend id at metadata step

**Approved sub-decisions:** (1) id `us_nature_gerda`; (2) skip `federal_muni_raw`; (3) directory geographic vintage = **2021** boundaries; (4) constituency directory = **one** table with `constituency_type`; (5) crosswalks (incl. `wkr_2021_to_2025_crosswalk`) deferred to phase 2 and live in the **GERDA dataset**, not the directory.

---

## Locked decisions (from user)

1. **Scope:** Core vote-result modules now — **federal, state, municipal, county-council (Kreistag), European**. Defer to phase 2: mayoral, Landrat, crosswalks, covariates, shapefiles, Meinungsbild/MRP.
2. **Party format:** **Long** — one row per (unit × election × [ballot] × party). Column names **in English**.
3. **Harmonization:** **Separate table per variant** (unharmonized, harmonized-2021/2023/2025).
4. **Directories:** create in `br_bd_diretorios_de` — **geography** (municipality, county, state), **constituency** (Wahlkreis), **party**.
5. **Validity checks (required before commit):** reshape introduces **no new 0s or nulls** (a source-`NA` party yields *no row*, never a 0/NULL row); vote totals/turnout **match published official statistics** for spot-checked elections.

---

## Data access

- Files live in the GitHub repo under `data/<module>/.../final/`, as **CSV and RDS** (Git LFS).
- Download CSV via the LFS media URL: `https://github.com/awiedem/german_election_data/raw/refs/heads/main/data/<path>.csv?download=` (raw.githubusercontent returns only the LFS pointer).
- Clean in Python (pandas), reshape wide→long, emit **all-STRING snappy Parquet** partitioned for upload; dbt `safe_cast`s to final types. (Per `us_medsl_elections` precedent.)

---

## Long-format schema (template)

Every vote-result table is **geography-first long**, all years/parties stacked. Columns:

**Unit-level (repeated on each party row):**
| column | type | source | notes |
|---|---|---|---|
| `year` | INT64 | election_year | partition column |
| `election_date` | DATE | election_date | |
| `id_municipality` / `id_county` / `id_constituency` | STRING | ags / county / wkr_nr | geo key by level; FK to `br_bd_diretorios_de` |
| `id_state` | STRING | state (2-digit) | FK to `…de.state` |
| `municipality_name` / `county_name` / `constituency_name` | STRING | *_name | |
| `ballot` | STRING | stimme | constituency tables only: `first_vote` / `second_vote` |
| `eligible_voters` | INT64 | eligible_voters | measurement_unit: `person` |
| `voters` | INT64 | number_voters | |
| `valid_votes` | INT64 | valid_votes | |
| `invalid_votes` | INT64 | invalid_votes | |
| `turnout` | FLOAT64 | turnout | proportion 0–1 |
| flags | STRING (bool 0/1) | flag_* | kept as documented diagnostics |

**Party-level (the melted rows):**
| column | type | notes |
|---|---|---|
| `party` | STRING | GERDA normalized party key; FK to `…de.party` |
| `votes` | INT64 | where source provides counts (else NULL) |
| `vote_share` | FLOAT64 | proportion; denominator per module (federal/European = voters; others = valid_votes) — recorded in table description |
| `seats` | INT64 | municipal / county-council-seats only (per-party) |
| `replaced_0_with_na` | STRING(bool) | municipal only (per-party flag) |

**Reshape rule (drives the validity check):**
- Melt only the individual-party / genuine-residual columns: individual parties **plus** `other`, `waehlergruppen`, `einzelbewerber` (+ state `einzelbewerber_*` variants).
- **Drop** cross-cutting aggregates from rows — `far_right`, `far_left`, `far_left_w_linke`, `cdu_csu` (only where separate `cdu`/`csu` exist), `total_vote_share` — so the invariant **Σ vote_share over parties ≈ 1** holds. Their party→group membership is captured as flags in the **party directory**, so any aggregate is recomputable by join.
- **Melt then drop NA** rows: a party that is `NA` in the wide file produces no row. Never emit `votes=0`/`vote_share=0`/NULL for an absent party. Check: `#non-NA party cells in wide == #party rows in long`.

---

## Phase-1 table list (~21 data tables)

### Federal (Bundestag)
| table slug | source file | level |
|---|---|---|
| `federal_municipality` | federal_muni_unharm | municipality |
| `federal_municipality_harmonized_2021` | federal_muni_harm_21 | municipality (2021) |
| `federal_municipality_harmonized_2025` | federal_muni_harm_25 | municipality (2025) |
| `federal_county` | federal_cty_unharm | county |
| `federal_county_harmonized_2021` | federal_cty_harm | county (2021) |
| `federal_constituency` | federal_wkr_unharm_long | constituency (already long) |
| `federal_constituency_2021_on_2025` | federal_wkr_2021_on_2025 | constituency (recomputed) |

*Skip `federal_muni_raw`* (pre-standardization ingest, non-analytic) — **confirm**.
*`wkr_2021_to_2025_crosswalk`* — small boundary-reform table; defer with phase-2 crosswalks or include as ref — **confirm**.

### State (Landtag)
| `state_municipality` | state_unharm | municipality |
| `state_municipality_harmonized_2021` | state_harm_21 | municipality (2021) |
| `state_municipality_harmonized_2023` | state_harm_23 | municipality (2023) |
| `state_municipality_harmonized_2025` | state_harm_25 | municipality (2025) |
| `state_constituency` | ltw_wkr_unharm_long | constituency (already long) |

*Skip legacy `state_harm`* (GERDA-deprecated, weighted-mean).

### Municipal (Gemeinderat / Kommunal)
| `municipal` | municipal_unharm | municipality (+ seats, replaced_0_with_na) |
| `municipal_harmonized_2021` | municipal_harm | municipality (2021) |
| `municipal_harmonized_2025` | municipal_harm_25 | municipality (2025) |

### County council (Kreistag)
| `county_council_municipality` | county_elec_unharm | municipality |
| `county_council_municipality_harmonized_2021` | county_elec_harm_21_muni | municipality (2021) |
| `county_council_county_harmonized_2021` | county_elec_harm_21_cty | county (2021) |
| `county_council_seats` | county_council_seats | county-year seat panel (long by party) |

### European (Europawahl)
| `european_municipality` | european_muni_unharm | municipality |
| `european_municipality_harmonized_2021` | european_muni_harm | municipality (2021) |

---

## Directories — `br_bd_diretorios_de` (new dataset, ~4–5 tables)

Built from GERDA's own crosswalk + election files (they carry ags↔name↔county↔state and the full party universe).

| table | key | attributes | source |
|---|---|---|---|
| `state` | `id_state` (2-digit) | name (de/en) | fixed 16-Länder list |
| `county` | `id_county` (5-digit) | name, `id_state` | crosswalk `cty_crosswalks` (2021 canonical) |
| `municipality` | `id_municipality` (8-digit AGS) | name, `id_county`, `id_state` | crosswalk `ags_crosswalks` |
| `constituency` | `id_constituency` | name, `constituency_type` (federal/state), `id_state` | wkr files |
| `party` | `party` (GERDA key) | label, `is_far_right`, `is_far_left`, `is_cdu_csu`, parlgov_id (where available) | union of party cols + GERDA `normalise_party`/`party_crosswalk` |

**Open directory sub-decisions:**
- **Canonical municipality/county vintage:** propose **2021** boundaries as the directory universe (GERDA's most complete harmonization; every module has a 2021 variant, not all have 2025). Unharmonized/other-vintage tables carry historical codes that will not all resolve to the directory — expected (that is what crosswalks are for). Tables harmonized to 2021 join cleanly. — **confirm 2021 vs 2025**.
- **Constituency:** one table with a `constituency_type` column vs. two tables (federal / state). Propose **one table** with the type column. — **confirm**.

---

## Raw data sources (metadata)

Two, per user:
1. **GERDA website / GitHub repo** — https://www.german-elections.com/ (data + code).
2. **The paper** — Nature Scientific Data 12:618, DOI 10.1038/s41597-025-04811-5.

---

## Execution order (incremental, checkpoint per module)

0. Finalize naming + open sub-decisions (this doc).
1. **Directories first** (`br_bd_diretorios_de`: state → county → municipality → constituency → party) — data tables FK to them.
2. **Federal module** end-to-end (download → clean/reshape → validity checks → BQ dev → dbt → validate) → **checkpoint + commit**.
3. Replicate for **state → municipal → county-council → European**, commit per module.
4. Metadata registration in dev (dataset `under_review`), verification checkpoint, then prod promotion on approval, PR, publish post-merge.

Commit discipline: `feat(de_gerda_elections): …` / `feat(br_bd_diretorios_de): …`, one logical unit per commit. Never commit data (`input/`, `output/`).

---

## Validity checks (per user requirement)

For each reshaped table:
- **No fabricated cells:** `#non-NA party cells (wide) == #party rows (long)`; assert no `vote_share` row is a fabricated 0/NULL.
- **Share invariant:** Σ `vote_share` over parties per unit ≈ 1 (federal/European ≈ voters/valid ratio) within tolerance; flag `total_vote_share`-style diagnostics.
- **Official cross-check:** for ≥2 spot elections (e.g. Bundestag 2021, 2025), aggregate `votes`/turnout to national/state level and compare against published Bundeswahlleiterin figures.
- Row-count parity vs. the codebook's stated dimensions per file.
