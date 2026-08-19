# Backend reference IDs — us_fec_campaign_finance

Resolved on the **staging** backend (`staging.backend.basedosdados.org`) on 2026-08-18.
The dev backend was returning 503 throughout, so staging was used per
`feedback_use_staging_backend`.

**IDs are per backend.** Re-resolve everything on prod before registering there, and
re-create the new organization and the new tag there too.

## Dataset

| Object | Value |
|---|---|
| dataset slug | `campaign_finance` |
| dataset id | `88e00739-5abb-4182-9f4e-16cfbe8f3019` |
| GCP dataset id | `us_fec_campaign_finance` |
| status at registration | `under_review` — `47208305-325a-4da9-9222-ac6849405b78` |

## Reference objects

| Category | Slug | ID |
|---|---|---|
| organization | `fec` (**created here**) | `e7b829ea-cab3-43c7-8288-1e2b910bfed2` |
| theme | `politics` | `c4e75f8c-29de-4b76-9607-657d4ac7f490` |
| license | `cc0` | `7fb71004-2abe-4fc8-a258-e2aac27c71d9` |
| availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |
| area | `us` | `61a2c232-c649-4b41-a5a3-1467b7393e11` |
| language | `en` | `0420c9c1-3ba3-4620-a074-56207eba5ae9` |
| status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |

## Tags

Eight existing tags plus one new. **`campaign-finance` was created** — no existing tag
covered the dataset's defining subject; the vocabulary had `financiamento` and
`eleicao` separately but nothing for campaign finance itself.

| Slug | ID | New? |
|---|---|---|
| `campaign-finance` | `d69e2d92-7f03-4b62-be5b-a6d83ef3573c` | **yes** |
| `eleicao` | `327ca750-b120-4d60-90ee-750a0f1fa79c` | no |
| `candidatura` | `039717e7-b20d-4fcb-8792-9d6cb8ec9484` | no |
| `partido` | `a4dcbfbb-cf4e-4b92-adfe-9b16ab2c5a15` | no |
| `doacao` | `76da3e48-725b-4a00-ab02-09e2eab9c74a` | no |
| `financiamento` | `25cde861-2c55-4c85-9c5a-48048953c6d4` | no |
| `despesa` | `2195dbbf-7f5f-437c-a71e-e1aab0ac2337` | no |
| `lobby` | `6fb25a8e-ca69-40a2-84ec-d5e5a9700b5d` | no |
| `transparencia` | `8b187427-519e-48cb-b0a6-5380086edf3b` | no |

## Raw data sources

One per table. `client._raw_source_id` raises when a table has two or more raw
sources, which breaks the recurring pipeline's poll at the first run, so the linkage
must stay one-to-one (`project_metadata_multi_raw_source_bug`).

| Source | ID | Table |
|---|---|---|
| Candidate Master (cn) | `45aab9c0-6951-4bdd-9fbc-b580a03cb157` | `candidate` |
| Committee Master (cm) | `66f850ae-fcd0-4a42-a9ca-15c1d1c51c63` | `committee` |
| Candidate-Committee Linkage (ccl) | `224d9e93-6ac2-4149-9eb3-538843d0411c` | `candidate_committee_link` |
| Contributions by Individuals (indiv) | `b6fd9e5e-25b3-48b6-9e2f-09f869264a3f` | `contribution_individual` |
| Contributions from Committees (pas2) | `486cde61-4b7d-4497-986f-669dc85cec18` | `contribution_committee` |
| Transactions between Committees (oth) | `719e8f38-dd24-43bb-bad5-46cfd553af8b` | `committee_transaction` |
| Operating Expenditures (oppexp) | `e0fb6447-fb3c-435f-ac37-e6767ce2af47` | `disbursement` |
| FEC code descriptions | `27df206f-e0b4-4304-b387-127282a938f4` | `dicionario` |

## Observation-level entities

Follows the `br_tse_eleicoes` precedent (`candidatos` → person/year/election_type).

| Entity | ID |
|---|---|
| `person` | `b4e76213-888b-40ea-b877-d82ce76d71a2` |
| `committee` | `214ea681-46e5-43c2-bc07-5d3a20de5481` |
| `donation` | `add96ebe-acb8-43e2-b665-43c03882027e` |
| `transaction` | `26887c0c-2a57-4808-b317-9c6cc63c2f3b` |
| `expenditure` | `5e4f445b-02e4-4eda-b06e-8d16fe2a8741` |
| `year` | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` |

Planned assignment:

| Table | Observation levels (column → entity) |
|---|---|
| `candidate` | candidate_id → person, cycle → year |
| `committee` | committee_id → committee, cycle → year |
| `candidate_committee_link` | candidate_id → person, committee_id → committee, cycle → year |
| `contribution_individual` | sub_id → donation, committee_id → committee, cycle → year |
| `contribution_committee` | sub_id → donation, committee_id → committee, cycle → year |
| `committee_transaction` | sub_id → transaction, committee_id → committee, cycle → year |
| `disbursement` | sub_id → expenditure, committee_id → committee, cycle → year |
| `dicionario` | none |

`update_column`'s boolean arguments default to `False`, so re-pass `is_partition=True`
when linking the observation level on `cycle`, or the flag is clobbered.

---

## Prod IDs (resolved 2026-08-19, before promotion)

**Not all reference UUIDs carry across from staging** — re-resolve, do not copy.
`cc0` and the `committee` entity differ; most others happen to match.

| Category | Slug | Prod ID | Same as staging? |
|---|---|---|---|
| license | `cc0` | `afd7b13d-98f5-4023-9cb3-e9b91b1962ca` | **NO** |
| entity | `committee` | `2ddfc4aa-8a47-44fe-bc03-4d2750bcda4b` | **NO** |
| theme | `politics` | `c4e75f8c-29de-4b76-9607-657d4ac7f490` | yes |
| area | `us` | `61a2c232-c649-4b41-a5a3-1467b7393e11` | yes |
| availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` | yes |
| status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` | yes |
| status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` | yes |
| language | `en` | `0420c9c1-3ba3-4620-a074-56207eba5ae9` | yes |
| entity | `person` | `b4e76213-888b-40ea-b877-d82ce76d71a2` | yes |
| entity | `donation` | `add96ebe-acb8-43e2-b665-43c03882027e` | yes |
| entity | `transaction` | `26887c0c-2a57-4808-b317-9c6cc63c2f3b` | yes |
| entity | `expenditure` | `5e4f445b-02e4-4eda-b06e-8d16fe2a8741` | yes |
| entity | `year` | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` | yes |
| entity | `week` | `485194cb-6656-492b-9ef5-436b22d2d202` | yes |
| entity | `month` | `f9659fea-e9bb-4177-9ca0-54076a8c0932` | yes |

**Must be created on prod** — neither exists there:
- organization `fec` (name/description as registered on staging, website
  <https://www.fec.gov/>, area `us`)
- tag `campaign-finance` (pt "financiamento de campanha", en "campaign finance",
  es "financiamiento de campaña")

The other eight tags (`eleicao`, `candidatura`, `partido`, `doacao`, `financiamento`,
`despesa`, `lobby`, `transparencia`) exist in the shared vocabulary but their prod IDs
still need resolving — they were not checked on prod.

## Prod registration order

Same sequence as staging, all with `env="prod"` and dataset `status.under_review`:
organization → tag → dataset → 8 raw data sources (one per table) → 8 tables →
columns via `bulk_upsert_columns` from `gen_columns_json.py` → observation levels →
link the grain columns (re-passing `is_partition=True` and `measurement_unit="year"`
on `year`, or they are clobbered) → cloud tables (`gcp_project_id="basedosdados"`) →
coverages (free on all 8, **pro on the 4 transaction tables**) → datetime ranges →
Updates → `reorder_columns` → deferred `raw_data_source_ids` link.

Coverage bounds to reuse (source-published maxima, not the data maxima — 71 typo'd
rows run past them):

| Table | free | pro |
|---|---|---|
| candidate / committee | 1980..2026 (interval 2) | — |
| candidate_committee_link | 2000..2026 (interval 2) | — |
| contribution_individual | 1975-07-28 .. 2026-01-31 | 2026-02-01 .. 2026-07-31 |
| contribution_committee | 1976-10-26 .. 2026-01-31 | 2026-02-01 .. 2026-07-31 |
| committee_transaction | 1976-10-26 .. 2026-01-31 | 2026-02-01 .. 2026-07-31 |
| disbursement | 1976-01-22 .. 2026-02-12 | 2026-02-13 .. 2026-08-12 |
