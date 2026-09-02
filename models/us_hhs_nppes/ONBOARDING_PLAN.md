# us_hhs_nppes — NPPES / NPI registry

Onboarding plan and design record. This is the **US healthcare provider
registry** — the analog of `br_rf_cnpj` (CNPJ), `fr_insee_sirene` (SIRENE) and
`au_ato_abr` (ABN) for health care, and the join key for CMS utilization data.

## Source

- Publisher: Centers for Medicare & Medicaid Services (CMS), National Plan and
  Provider Enumeration System (NPPES), US Department of Health and Human Services.
- Landing: https://download.cms.gov/nppes/NPI_Files.html
- Data: `NPPES_Data_Dissemination_<Month>_<Year>_V2.zip` — the **monthly full
  replacement file**, ~1.1 GB zipped / ~11.9 GB unzipped.
- License: **US Government public domain**. The file is published under the
  NPPES Data Dissemination Notice (CMS-6060-N, Federal Register, 30 May 2007) as
  FOIA-disclosable data, free to download and reuse with no registration or
  licence. Registered as `cc0`, the house slug for US federal public domain
  (matching `us_cms_open_payments` and `us_fec_campaign_finance`).
- Cadence: **monthly full snapshot**, posted in the first half of the month
  (August 2026 landed on the 10th). Weekly incrementals also exist and are
  deliberately ignored — each monthly file supersedes them.
- Snapshot onboarded: **extraction_date = 2026-08-09** (the end of the source
  file name's date range, which is the data cutoff; the bundle was posted
  2026-08-10).

### Bundle contents

| Member | Size | Becomes |
|---|---|---|
| `npidata_pfile_20050523-20260809.csv` | 11.6 GB, 330 cols | `provider`, `taxonomy`, `other_identifier` |
| `othername_pfile_…csv` | 50 MB | `other_name` |
| `pl_pfile_…csv` | 119 MB | `practice_location` |
| `endpoint_pfile_…csv` | 125 MB | `endpoint` |
| `NPPES_Data_Dissemination_Readme_v.2.pdf` | 518 KB | auxiliary files |
| `NPPES_Data_Dissemination_CodeValues.pdf` | 2.8 MB | auxiliary files + `dicionario` |

## Data model (CNPJ-style stacked snapshots)

Each monthly file is a full photograph of the registry. We **stack** them and
keep an `extraction_date` DATE partition (mirrors CNPJ `data_referencia` and
`au_ato_abr`). The six data tables are `materialized="incremental"`, partitioned
by `extraction_date` at day granularity, so successive months accumulate into a
panel rather than overwriting.

| Table | Grain | Rows (2026-08-09) |
|---|---|---|
| `provider` | one row per NPI per snapshot | 9,726,865 |
| `taxonomy` | NPI × taxonomy slot (≤15) | 12,171,227 |
| `other_identifier` | NPI × other-identifier slot (≤50) | 2,758,928 |
| `other_name` | one row per organization other name | 859,461 |
| `practice_location` | one row per secondary practice location | 1,241,921 |
| `endpoint` | one row per electronic endpoint | 603,391 |
| `dicionario` | code → label | 992 |

The real work is normalising the source's **three repeating groups**: 330
columns = 55 flat + taxonomy×15 (4 columns each) + other identifier×50
(4 columns each) + taxonomy group×15. Those 275 columns become two long tables.

### Decisions

- **Column names in English** (`year`-style rule for an English-language
  dataset), `_id` suffix convention. `npi` keeps its own name — it *is* the
  National Provider Identifier.
- **Three source columns dropped**, all information-free:
  `Employer Identification Number (EIN)` and `Parent Organization TIN` are
  suppressed by CMS (every row literally reads `<UNAVAIL>`), and
  `NPI Deactivation Reason Code` is documented as "not publicly disseminated"
  and is empty in every row.
- **CMS masks** for numbers providers wrongly entered into FOIA-disclosable
  fields (`$$$$$$$$$` for SSN, `*********` for ITIN, `=========` for EIN) are
  mapped to NULL in `license_number` and `other_identifier`. They carry no
  information, and keeping them would republish the shape of suppressed
  identifiers.
- **Deactivated NPIs are near-empty rows.** Verified on a 400k-row sample: a row
  with a blank `entity_type_code` *always* has a `deactivation_date` (0
  exceptions), and carries nothing else. Deactivated-then-reactivated NPIs keep
  full data. This is why the flat columns cluster at a ~91% fill rate.
- **`primary_taxonomy_code` is derived** on `provider`: the taxonomy code whose
  primary switch is `Y`. Verified: no NPI has more than one.
- **`taxonomy_group` is split** into `taxonomy_group_code` (the 10-character
  code) and `taxonomy_group_name`, because the source concatenates them and the
  same code appears with more than one wording
  (`193400000X SINGLE SPECIALTY  GROUP` vs `193400000X MULTIPLE SINGLE SPECIALTY GROUP`).
- **Redundant label columns dropped** from `endpoint`: the source ships
  `Endpoint Type Description`, `Use Description` and `Content Description`
  alongside their codes. The codes are dictionary-covered, so the labels live in
  `dicionario` instead of being duplicated on every row.
- **Byte-identical duplicate rows** exist in the source's `pl` (19,627) and
  `endpoint` (15,769) files. The models collapse them with `SELECT DISTINCT`,
  the same treatment `au_ato_abr__other_name` uses.
- **Dates** are `MM/DD/YYYY` in the source and are normalised to ISO in the
  transform; staging stays all-STRING and the dbt models `safe_cast` to DATE.
- **Sequence columns are STRING** (`taxonomy_sequence`, `identifier_sequence`)
  per the type-by-arithmetic-meaning rule: they are slot numbers, not quantities.
- **Geography** links to `br_bd_diretorios_us.state:abbreviation` — NPPES carries
  USPS two-letter abbreviations, not FIPS. No `relationships` test is added: the
  state fields also hold Canadian provinces, US military codes (AA/AE/AP) and
  `ZZ` (foreign), which are outside the directory by design. 145 distinct values
  appear against an official list of 60.

### What the dbt tests caught

The first `dbt test` run failed three tests, and chasing them turned up two
defects that no aggregate row-count check would have found.

1. **`endpoint` was a column named after its own table.** dbt compiles model
   refs as `` `proj`.`ds`.`endpoint` ``, so the trailing identifier is also the
   table alias and an unqualified `endpoint` in a test resolves to the **whole
   row struct**, not the column. Its `not_null` test therefore passed while the
   column had 7 NULLs, and its uniqueness test passed while the same key had
   13,598 duplicate groups. The column is now `endpoint_address`.

2. **The CMS mask was applied too widely.** The Data Dissemination readme
   documents masking (`$$$$$$$$$`, `*********`, `=========`) for exactly two
   field families — provider license number and other provider identifier. The
   transform applied it to every column, which nulled seven legitimate (if junk)
   `Endpoint` values that are literally `*`, `**`, `***` or `$$$$`. The mask is
   now scoped to the two documented fields.

Three smaller corrections came from the same run:

- `PW` (Palau) appears as a **country** code, but the source's own country table
  (Code Values section 1.10) lists it only as a state code. Eight territory and
  freely-associated-state codes that are valid ISO 3166-1 alpha-2 countries were
  added to the dictionary; `DC` and `ZZ` were not, since they are not countries.
- `other_name` needs `created_date` in its key: the source records the same
  (npi, name, type) with different creation dates, 135,926 times.
- `practice_location` and `endpoint` have no source key at all. Their keys are
  the full column list, which after the model's `SELECT DISTINCT` is unique by
  construction and guards that `DISTINCT` against being removed.

### Taxonomy code labels are deliberately absent — open decision

`taxonomy_code` is stored, but its **labels are not**. The Health Care Provider
Taxonomy code set is maintained by NUCC, is copyrighted by the American Medical
Association, and its own distribution page states that "for commercial use,
including sales or licensing, a license must be obtained". Data Basis operates a
paid BD Pro tier, so redistributing the code set is a licensing question, not a
technical one.

Two ways forward, for the maintainers to choose:

1. Obtain the NUCC/AMA licence and add a `taxonomy_code` reference table.
2. Use the **CMS Medicare Provider and Supplier Taxonomy Crosswalk**
   (data.cms.gov, US public domain), which carries taxonomy code → description
   for the Medicare-eligible subset. Narrower coverage, no licence question.

Until then the codes join cleanly to any external NUCC copy the user holds.

## Tier: PartBdpro

Monthly refresh ⇒ by the house rule, the recent snapshot window is paywalled to
BD Pro and older snapshots stay free. The rolling window and the BigQuery Row
Access Policies are applied by the recurring pipeline
(`register_table_materialization_task`), not by the static onboard. At onboarding
there is a single snapshot, so the free/pro split is degenerate; both Coverages
are still created up front, because `assert_coverage_topology` hard-fails a
`part_bdpro` run when the pro Coverage is missing.

`free_lag` is set to 6 months, matching `br_rf_cnpj` and `au_ato_abr`.
**Confirm it before arming**, because with a single onboarded snapshot the
6-month lag puts `free_end` (2026-02-09) *before* the only snapshot (2026-08-09),
which is the inverted-free-range condition that bit
`br_senado_dados_abertos_administrativos` at go-live. Either shorten the lag
(e.g. one month) or wait until several snapshots have accumulated. At onboarding
the free Coverage holds the single snapshot and the pro Coverage exists with no
range, which is the honest state: nothing is paywalled until the pipeline runs.

## Files

- `code/build_architecture.py` — the single column spec; emits `columns_json/`
  (trilingual, for `bulk_upsert_columns`) and `architecture/` (BD source of truth).
- `code/build_dicionario.py` — builds `code/dicionario.csv` from the source's own
  Code Values document. Run once per code-values revision.
- `code/build_dbt_models.py` — generates the `.sql` models from the architecture,
  so column order cannot drift.
- `code/build_schema_yml.py` — generates `schema.yml`; `ignore_values` is
  *measured* from the cleaned output, not guessed.
- `code/clean_data.py` — one-shot bootstrap; a thin CLI over the shared transform.
- `code/upload.py` — uploads the cleaned parquet to `basedosdados-dev` staging.
- `pipelines/datasets/us_hhs_nppes/utils.py` — the shared, Prefect-free transform.

## Recurring pipeline (step 12)

`pipelines/datasets/us_hhs_nppes/` — monthly Prefect 3 flow `us_hhs_nppes_flow`.

- **Poll-first**: discover the monthly link from the listing page, HEAD it, and
  compare `Last-Modified` against `Table.Update.latest`
  (`compare_against="table_update"`). The ~1.1 GB payload is downloaded only when
  CMS republishes, so a scheduled run between releases is a cheap no-op.
- **Stacking**: staging upload `dump_mode="overwrite"` (current snapshot); the
  **incremental** dbt models append the new `extraction_date` partition to prod.
- **Shared transform**: `utils.py` holds the pure download + clean functions and
  the bootstrap imports them, so the two can never drift.
- **Run-then-test**: every table is `dbt run` before any table is `dbt test`ed,
  in each environment, because `custom_dictionary_coverage` reads the sibling
  `dicionario` model.
- **Schedule**: `23 15 8,10,12,14,16 * *` America/Sao_Paulo — a free minute, on
  the days CMS typically posts.
- **Deploy**: the PR needs the **`deploy-flow`** label for the dev-pool
  registration; then a dev run with
  `{materialize_to_prod: False, update_metadata: False, force_run: True}` is the
  definition of done. Merging deploys to prod **paused**; arming is a manual tick
  in Django admin.

## Scratch data

`~/Downloads/us_hhs_nppes_data/` (`input/`, `output/`) — never in the repo or
under Dropbox. Deleted at step 14.
