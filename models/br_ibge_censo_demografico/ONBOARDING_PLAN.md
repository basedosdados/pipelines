# Onboarding plan — Censo Demográfico 2022 (microdados públicos)

**Dataset:** `br_ibge_censo_demografico` (already published; 1970–2010 sample series)
**Status:** in progress · 2026-09-02
**Branch:** `data/br_ibge_censo_demografico`
**Org:** IBGE · **Cadence:** decennial · **No Prefect pipeline**

This run extends the published dataset with the 2022 **public** sample
microdata. All local BigQuery uses GCP project `sandbox-507414` and SA
`localall@sandbox-507414.iam.gserviceaccount.com`. Official
`basedosdados-dev` / `table-approve` is a later, explicit step.

## Settled decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Dataset home | `br_ibge_censo_demografico`, not `br_ibge_censo_2022` |
| 2 | Access level | Public FTP only (UF is max geography) |
| 3 | Record types | DOMI, PESS, FAMI, MORT (no emigração file in the public zip) |
| 4 | Column names | IBGE codes lowercased (`d0130`, `p0150`); BD names only for geo/weights/ids |
| 5 | Partition | hive `ano=2022/sigla_uf=<UF>/` — those columns are path-only in parquet |
| 6 | Staging types | all-STRING; types live in dbt `safe_cast` |
| 7 | Dictionary | extend existing `dicionario` with `cobertura_temporal=2022` (do not add a second table) |
| 8 | BQ target this run | `sandbox-507414` only |

## Tables

| Slug | IBGE sheet / CSV | Grain |
|------|------------------|-------|
| `microdados_domicilio_2022` | DOMI / `Domicilios_{uf}_publico.csv` | one row per household |
| `microdados_pessoa_2022` | PESS / `Pessoas_{uf}_publico.csv` | one row per person |
| `microdados_familia_2022` | FAMI / `Familia_{uf}_publico.csv` | one row per family |
| `microdados_mortalidade_2022` | MORT / `Mortalidade_{uf}_publico.csv` | one row per reported death |

Public-file limits (must appear in every table description):

- Most detailed geography is **UF**, not município / área de ponderação.
- Only records with disclosure risk &lt; 20%; quasi-identifiers omitted.
- 50% subsample of households that had a 100% sampling fraction.
- Age recoded to 5-year groups; local suppression of sex/age on high-risk rows.

## Source

- CSV zips: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Microdados_e_Areas_de_Ponderacao/Microdados_de_acesso_Publico/csv/
- Docs: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Microdados_e_Areas_de_Ponderacao/Documentacao/Layout%20e%20dicion%C3%A1rio
- Released 2026-08-31. Prefer per-UF zips, not `Todas_as_UFs.zip`.

## Scratch

Local working copy was `~/Downloads/br_ibge_censo_demografico_data/`
(`input/`, `output/`, `docs/`). Process one UF at a time; delete each zip
after that UF is cleaned.

**Durable copy (sandbox GCS):**
`gs://sandbox-507414-br-ibge-censo-demografico/br_ibge_censo_demografico/`
(112 objects: hive parquet + layout + dicionário + `row_counts.json`).
Local scratch was deleted after a size-verified sync (`code/sync_gcs.py`).

## BigQuery (this machine)

| | Value |
|---|---|
| Project | `sandbox-507414` |
| Staging | `sandbox-507414.br_ibge_censo_demografico_staging.<table>` |
| Models | `sandbox-507414.br_ibge_censo_demografico.<table>` |
| Credentials | `~/.basedosdados/credentials/sandbox-507414.json` (never committed) |

Do **not** call `bd.Table` (that lands in `basedosdados-dev`). Skip the
`allUsers` GRANT post-hook. Directory FK tests wait for the official lake.

Official-lake upload of `dicionario` must **append** 2022 rows to the
historical staging table, not replace 1970–2010.

## Drive / metadata

Drive MCP and Data Basis backend MCP were not authenticated in this
session. Architecture lives as local CSVs under `code/architecture/`
(parsed from the IBGE public layout). Metadata registration is blocked
until those MCPs work. Cloud-table URLs must name `basedosdados` /
`basedosdados-dev`, never `sandbox-507414`.

## Checklist

- [x] Context: source URLs, record types, public-file limits
- [x] Architecture CSVs from IBGE layout (Drive sheets deferred)
- [x] Download + clean all 27 UFs
  (domicílio 7,689,914 · pessoa 21,538,508 · família 6,550,107 · mortalidade 430,961)
- [x] Upload staging to `sandbox-507414` (local↔BQ counts match)
- [x] dbt models + `schema.yml`
- [x] dbt run/test on sandbox (no GRANT, no directory FKs)
  Unique + key not-null passed on all 4 tables. Directory FKs deferred.
  `not_null_proportion` omitted (skip-pattern sparse, same as 2010).
- [ ] Auxiliary GCS bundles (deferred — needs BD credentials)
- [ ] Discover + metadata dev
- [ ] Checkpoint / approval
- [ ] Metadata prod
- [ ] PR
- [ ] Official lake + `table-approve` (later)
- [ ] Cleanup `~/Downloads/br_ibge_censo_demografico_data/`
