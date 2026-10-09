# Backend reference IDs

The development backend has been returning 503 since 2026-07-09, so pre-production
metadata is registered on **staging** (`staging.backend.basedosdados.org`). Reference
UUIDs are preserved between staging and prod, so the IDs below are reused on both.

## Dataset

| Field | Value |
|---|---|
| Dataset slug | `open_payments` |
| GCP dataset id | `us_cms_open_payments` |
| Organization | `centers_for_medicare_medicaid_services_cms` — `893ca241-c99e-4dd4-98d5-6e9172179f2e` |
| Themes | `health` `1c0535e3-d0ad-47c0-a324-727aa9b1d622`, `economics` `ad6a413a-e882-4dd6-a497-8a62eec8511b`, `government` `6dd730bb-89ab-4dba-a1bf-a25ca1c35003` |
| Status on creation | `under_review` — `47208305-325a-4da9-9222-ac6849405b78` |
| Status published | `e16221de-ac30-4926-83d3-de219998dab3` |

No Open Payments dataset exists on either backend, so this is a fresh creation.

## Raw data source

| Field | Value |
|---|---|
| License | `cc0` — `7fb71004-2abe-4fc8-a258-e2aac27c71d9` |
| Availability | `online` — `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |

CMS publishes Open Payments under `https://www.usa.gov/government-works`, the US
government works notice: a work of the federal government, not subject to copyright
in the United States. The Data Basis vocabulary has no entry for that notice, so it
takes `cc0`, following `us_fed_fred` and `us_cfpb_hmda`, the two closest peers.

## Observation level entities

| Entity | ID |
|---|---|
| payment | `7cd9f097-f7ad-4b8a-8c07-746b6fbef450` |
| person | `b4e76213-888b-40ea-b877-d82ce76d71a2` |
| hospital | `72cd18a6-42c4-4fbb-987e-cb26272a5c14` |
| company | `b585c285-3ad7-4b86-9c36-6195e4760a46` |
| state | `839765a7-9c7a-44bd-bb88-357cedba03f6` |
| occupation | `859cabcb-db31-4d57-aa3d-ca6b6d840b9c` |
| year | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` |

## Measurement units

`usd`, `year` and `person` all exist in the backend vocabulary. There is no unit for
a plain count, so transaction and record counts carry no unit and say so in their
`observations` field.
