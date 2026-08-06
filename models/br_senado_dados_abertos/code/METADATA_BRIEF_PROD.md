# Metadata registration — PROD promotion (br_senado_dados_abertos)

This is step 10: register the SAME metadata in the **prod** backend (`env="prod"`) that was
registered in staging. The dataset does NOT yet exist on prod (confirmed). Follow
`METADATA_BRIEF.md` for the full per-table sequence, OL entities, and coverage ranges — with
these PROD OVERRIDES:

## Overrides vs the staging brief
- **`env="prod"`** on every call.
- **Dataset status = under_review** (NOT published) — cloud tables will point at prod tables
  (`basedosdados.br_senado_dados_abertos.*`) that do not exist yet; they materialize when the
  onboarding PR merges (table-approve). Do NOT publish.
- **Cloud tables: `gcp_project_id="basedosdados"`** (prod), gcp_dataset_id="br_senado_dados_abertos".
- **published_by_ids / data_cleaned_by_ids = account id `4`** (rdahis@basedosdados.org on prod).
- Confirmed prod reference IDs (same UUIDs as staging): organization `senado`
  `197881e3-9074-4c39-a749-b150594697b4`; status under_review `47208305-325a-4da9-9222-ac6849405b78`;
  theme government `6dd730bb-89ab-4dba-a1bf-a25ca1c35003`, politics `c4e75f8c-29de-4b76-9607-657d4ac7f490`.
- **Resolve these on PROD with `lookup_id(category=…, slug=…, env="prod")`** (do NOT reuse staging
  UUIDs — area/license/entities are not guaranteed identical): area `br`; license `cc_by`
  (raw-source license — user approved CC BY); and the 7 entities used as observation levels:
  `year`, `vote`, `person`, `party`, `bill`, `committee`, `caucus`. If `lookup_id` for an entity
  returns nothing, fall back to the staging UUIDs in METADATA_BRIEF.md (they matched for org/status/theme).

## Everything else is identical to METADATA_BRIEF.md
Same 10 tables, same column set (from `code/columns_json/<slug>.json`), same names/descriptions
(from `architecture_spec.py`), same OL entity→grain-column links, same is_partition on `ano`,
same free (is_closed=false) br coverages + datetime_ranges, same day-frequency table updates
(latest="2026-08-06T00:00:00"), same raw data source
(`https://legis.senado.leg.br/dadosabertos`, online, free, contains_api, no registration, language pt),
linked to all 10 tables via the deferred second create_update_table pass, same table display order.

## Report back
Run `get_dataset("br_senado_dados_abertos", env="prod")` and report dataset id, per-table
column/OL/coverage/cloud-table status, the raw source id + resolved license slug, and any errors.
