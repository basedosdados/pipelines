# au_abs_migration

Australian migration statistics from the ABS: overseas migration (cat. 3407.0,
*Overseas Migration*) and interstate migration (from *National, state and
territory population*). Phase 1 covers the national and state grain; the
sub-state (SA2 and above) regional series is a later phase.

## Sources

Two kinds, because neither alone is complete.

| Source | What only it has |
|---|---|
| Time-series spreadsheets `34070DO001`–`34070DO004` | country of birth; the annual visa series back to 2004-05 |
| SDMX dataflows `NOM_FY`, `NOM_CY`, `OMAD_VISA`, `NIM_FY`, `NIM_CY` (ABS Data API) | age and sex; quarterly visa detail; interstate migration back to 1996-97 |

Full SACC country names come from the `CL_ERP_COB` codelist — the spreadsheets
ship them abbreviated ("PNG", "S & E Afr, nec").

## Tables

| Table | Grain | Rows |
|---|---|---|
| `overseas_country_of_birth_australia` | year × country | 5,250 |
| `overseas_country_of_birth_state` | year × state × country | 42,000 |
| `overseas_age_sex_australia` / `_state` | financial year × age × sex (× state) | 8,505 |
| `overseas_age_sex_australia_calendar_year` / `_state_calendar_year` | calendar year × age × sex (× state) | 8,505 |
| `overseas_visa_australia` / `_state` | financial year × visa group (× state) | 3,213 |
| `overseas_visa_quarter_australia` / `_state` | quarter × visa group (× state) | 12,636 |
| `interstate_age_sex_australia` / `_state` | financial year × age × sex (× state) | 13,311 |
| `interstate_age_sex_australia_calendar_year` / `_state_calendar_year` | calendar year × age × sex (× state) | 14,790 |
| `dicionario` | coded value → label | 722 |

108,932 rows in total.

## Design decisions

**Geography is split by granularity.** ABS rounds every value to the nearest 10,
so the states do not sum to the published Australia figure. The two grains
therefore live in separate tables — `_australia` and `_state` — rather than
sharing a column in which the national row would look like just another region.
`state_id` carries the ASGS code and links to `br_bd_diretorios_au.state`.

**`year` is the start year of the financial year.** 2004 means 2004-05. The
SDMX dataflows label a financial year by its *end* year, so the cleaning step
subtracts one; `validate_country_totals` checks the result against the
spreadsheet's own totals for all 21 years.

**Country of birth carries both codes.** `country_of_birth_id` is the SACC 2016
code as published, always present and covered by the `dicionario`;
`country_iso3_code` is resolved to ISO 3166-1 alpha-3 and links to
`br_bd_diretorios_mundo.pais`. 243 of the 250 published codes resolve. The seven
that do not are the five residual "nec"/"nfd" categories, "Inadequately
Described", Kosovo and Spanish North Africa (no ISO 3166-1 code). Two mappings
are deliberate approximations, both recorded in `code/sacc_iso3.csv`: SACC 2100
spans the United Kingdom together with the Channel Islands and the Isle of Man
and is mapped to GBR, and the seven Antarctic territorial claims all map to ATA.
The mapping is exact-name-match plus a hand-reviewed alias list — never fuzzy
matching, which confuses Niger with Nigeria and the two Congos with each other.

## Traps

**The annual and quarterly visa tables are different vintages.** The annual
tables come from the December 2025 annual release; `OMAD_VISA` is refreshed with
each quarterly population release and therefore carries a later revision of the
preliminary year. On 2004-05 to 2023-24 the two agree cell for cell — which is
what verifies the spreadsheet's label-to-code mapping — and they differ only in
2024-25. The annual tables are kept on the annual vintage so that they stay
consistent with the country-of-birth and age/sex tables, which are all from the
same release.

**Visa code `03` (Other Visas) exists only in the quarterly tables.** The
spreadsheet folds it into the total.

**The interstate calendar-year table has one more region than its financial-year
twin.** `NIM_CY` publishes Other Territories (state code 9); `NIM_FY` does not.

**Codes `1020`, `1040` and `1041` are visa aggregates**, `TOT` is the age total
and `3` is the sex total. Filter them out before summing, or you double-count.

**Age code `A59` means 5-9 years, not 59.** ABS's own coding; the `dicionario`
carries the labels.

## Where the code lives

`models/au_abs_migration/code/`:

- `download.py` — spreadsheets, dataflows and the country codelist into
  `$AU_ABS_MIGRATION_DATA/input` (default `~/Downloads/au_abs_migration_data`)
- `gen_architecture.py` — writes `architecture/*.csv`, the schema source of truth
- `build_country_mapping.py` — writes `sacc_iso3.csv` (SACC → ISO3)
- `clean.py` — parses everything, validates, writes all-STRING partitioned parquet
- `gen_dbt.py` — writes the 15 models and `schema.yml` from the architecture
  (pre-commit's `sqlfmt` and `yamlfix` reformat them afterwards, so a rerun shows a
  formatting diff until the hooks run again)
- `metadata.py` — registers the dataset in the backend; `--publish` flips the status
- `upload.py` — uploads to `basedosdados-dev.au_abs_migration_staging`

No recurring pipeline: the release is annual and the dataset was onboarded as a
one-shot. `OMAD_VISA` does refresh quarterly, so the quarterly tables are the
part that ages first.

## Licence

Creative Commons Attribution 4.0 International (ABS default).
