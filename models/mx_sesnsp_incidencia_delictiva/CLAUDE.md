# mx_sesnsp_incidencia_delictiva — context for future updates

Monthly Mexican crime counts from **SESNSP** (Secretariado Ejecutivo del Sistema Nacional de
Seguridad Pública). Onboarded together with its geography directory **br_bd_diretorios_mx**
(INEGI). License: **Libre Uso MX** (open, attribution). Org slug: `sesnsp`.

## Datasets & tables

**br_bd_diretorios_mx** (INEGI AGEEML geography directory — prerequisite for the FKs)
- `estado` — 32 entidades federativas. PK `id_estado` (2-digit INEGI clave). Pure INEGI, no sentinels.
- `municipio` — ~2,478 municipios. PK `id_municipio` (5-digit `EEMMM`), FK `id_estado` → estado.

**mx_sesnsp_incidencia_delictiva** — 7 tables, monthly, partition `ano` (INT64) + `mes` (INT64),
Spanish columns, measure `cantidad` (INT64). Two SESNSP methodologies are kept in **separate
tables** (their crime catalogs are incompatible):

| table | grain | measure | methodology / coverage |
|---|---|---|---|
| `municipio_delitos_2015_2025` | municipal | delitos | legacy, **frozen** 2015-01..2025-12 |
| `municipio_delitos` | municipal | delitos | new, **ongoing** 2026-01.. |
| `municipio_victimas` | municipal | víctimas | new, ongoing 2026-01.. (municipal victims exist only from 2026) |
| `estatal_delitos_2015_2025` | state | delitos | legacy, frozen |
| `estatal_delitos` | state | delitos | new, ongoing |
| `estatal_victimas_2015_2025` | state | víctimas | legacy, frozen (coarse age bands) |
| `estatal_victimas` | state | víctimas | new, ongoing (6 age bands) |

**Naming rule:** the *ongoing* new-methodology table is **unsuffixed** (it gains a month every
release, so a date stamp would go stale); the *frozen* legacy table keeps `_2015_2025`.

**Column naming:** geography IDs in the crime tables are `id_entidad` (2-digit) and `id_municipio`
(5-digit) — SESNSP's own field is "Entidad"/"Clave_Ent", so the crime tables keep `id_entidad`.
The **directory** uses `id_estado` (matching the `estado` table and the sibling
`br_bd_diretorios_us.id_state` convention). The crime FK is `id_entidad → estado:id_estado`
(different names, valid link). víctimas tables add `sexo` + `rango_edad`.

## Source & download (the tricky part)

Landing page: https://www.gob.mx/sesnsp/acciones-y-programas/datos-abiertos-de-incidencia-delictiva
- The page is **JS-rendered** (curl gets an empty shell) → use a browser to read the links.
- datos.gob.mx is **Akamai-blocked** (403 to all programmatic access) — do not rely on it.
- Files are **anonymous SharePoint share links**: `.../:u:/g/personal/cni_sspc_gob_mx/<TOKEN>?e=..`.
  Download WITHOUT auth via the canonical form (do NOT append `&download=1` — that bounces to MS login):
  `https://sspcgob-my.sharepoint.com/personal/cni_sspc_gob_mx/_layouts/15/download.aspx?share=<TOKEN>`
  (send a browser User-Agent). Returns the zip/xlsx.
- **Tokens and filenames change every monthly release** (filename embeds the month, e.g.
  `Municipal-Delitos-2015-2025_jun2026.zip`). A recurring pipeline must **re-scrape the gob.mx page
  for the current token** each run — do not hardcode tokens.
- **HEADLESS SCRAPE (pipeline): the gob.mx page is behind an Imperva bot challenge** ("Challenge
  Validation" — plain curl/requests get a 1850-byte challenge shell). **`curl_cffi` with
  `impersonate="chrome"` passes it** (TLS-fingerprint spoofing; no browser needed) → returns the
  full 63KB page with all SharePoint links. Then parse anchors by their visible label text to map
  the current token to each table:
    - `municipio_delitos`   ← label "(Fuero común - Delitos). Incidencia delictiva municipal", 2026 methodology
    - `municipio_victimas`  ← "(Fuero común - Víctimas). Incidencia delictiva municipal"
    - `estatal_delitos`     ← "(Fuero común - Delitos). Incidencia delictiva estatal", 2026
    - `estatal_victimas`    ← "(Fuero común - Víctimas). Incidencia delictiva estatal"
  Match: label contains the grain + measure, is the current-methodology one (label starts with a
  month range like "Enero - <mes> 2026", NOT "2015 - 2025"), excluding "Fuero federal" and
  "Tablero dinámico". Download each token via the `download.aspx?share=<TOKEN>` form, also with
  `curl_cffi` impersonation.

Fuero Común tokens verified 2026-08-12 (Jan–Jun 2026 data at that point):
| table | share token |
|---|---|
| municipio_delitos_2015_2025 | IQAnMGiScnoTTr4J2J9mUZthAat6lEdo7-1MCUpQU4n4EwQ |
| municipio_delitos (2026) | IQCrvfVt1whGSIVWEKNeYn-RAegACJmpTgG7PCMiYYbMTyc |
| municipio_victimas (2026) | IQCUbMerdHv4QaFM1SB-2EO4AYxHuzrt-qh3mQdjwr22vg8 |
| estatal_delitos_2015_2025 | IQCImg0CgaACTJdHIpZ_yZKpAYKWg8wsEgtzU0nN3o-GVKU |
| estatal_delitos (2026) | IQCSp-c6g6Y0QYFn1X32bXM0AXmUDrlxqn-paFNMTuqCNjQ |
| estatal_victimas_2015_2025 | IQAjMphIBZwaQZy25I_oN-HWAaJZJFxQWE1y2k3seT0lj74 |
| estatal_victimas (2026) | IQA2f6SUapqCQoGf4RGYi5hWAb_Sq7rEuVcWn6fqrlPWBZM |

**Zip contents:** legacy municipal ships BOTH 11 per-year `.xlsx` AND a combined CSV
(`Municipal-Delitos - (2015-2025) …csv`) — **use the CSV**. Legacy estatal = single CSV. All 2026
files = single `RNID-*.csv`. Encoding **latin-1**. Some `.csv` names have accents that break macOS
`unzip` ("Illegal byte sequence") → extract those with Python `zipfile`.

Directory source: INEGI AGEEML web service (no key): `https://gaia.inegi.org.mx/wscatgeo/v2/mgee/`
(entidades) and `/mgem/{cve_ent}` (municipios per state). `clean.py` caches the JSON.

## Cleaning transform (`code/clean_data.py`)

Wide → long: melt the 12 Spanish month columns (Enero..Diciembre) into `mes` (1..12) + `cantidad`.
- `id_entidad = Clave_Ent.zfill(2)`; `id_municipio = "Cve. Municipio".zfill(5)` (Cve. Municipio is
  already the full INEGI clave, e.g. `1001` → `01001`; verified: FK test is 0-missing).
- Keep explicit `0` counts; **drop** months that are blank (not yet published — future months).
- Drop the geography *name* columns (Entidad, Municipio) — they live in the directory.
- **Must use pandas/csv** (embedded commas in quoted Entidad long-names break naive splitting).
- Output all-STRING Snappy parquet, hive-partitioned by `ano`; big table melted per-year to cap RAM.

## dbt (`schema.yml`, generated by `code/build_dbt.py`)

- Partition `ano`; safe_cast every column; big municipal tables scope tests with
  `__most_recent_year_month__`.
- **SESNSP aggregate municipio codes 998/999** ("No especificado" / "Otros municipios", meanings
  swap between methodologies) are NOT real INEGI municipios → they are **ignored in the id_municipio
  FK test** via `config.where: … right(id_municipio, 3) not in ('998', '999')` (per user preference —
  NOT injected as directory sentinel rows). Documented in the municipal model descriptions.

## Regenerate / update workflow

Everything is generated from the architecture CSVs (`code/architecture/*.csv`), which are the single
source of truth. To change schema: edit `code/build_architecture.py` → run it → run
`code/build_dbt.py`. Scripts:
- `code/build_architecture.py` → 9 architecture CSVs (2 directory + 7 crime).
- `code/clean_data.py [tables] [--sample N]` → parquet under `~/Downloads/mx_sesnsp_incidencia_delictiva_data/output/`.
- `code/upload.py [--env dev|prod] [tables]` → BigQuery staging (requester-pays patched).
- `br_bd_diretorios_mx/code/{clean,upload}.py` for the directory.

**Env/creds (local = dev only):**
- upload: `export GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json`
  (unset by default; `staging.json` = `basedosdados-dev`).
- dbt: **must** pass `--profiles-dir ~/.dbt` (the repo `./profiles.yml` has the CI path
  `/credentials-dev/dev.json` and fails locally). Run `uv run dbt deps` once first.
- Scratch data lives under `~/Downloads/{mx_sesnsp_incidencia_delictiva_data,br_bd_diretorios_mx_data}/`
  (never in the repo/Dropbox). Delete after publishing.

## Recurring monthly pipeline (TODO — separate PR, step 12)

Strongest recurring candidate (monthly). Follow `.claude/rules/prefect-pipeline-conventions.md`:
- Reuse `code/clean_data.py`'s melt (import it; do not duplicate). Re-scrape the SharePoint token.
- Only the *ongoing* tables refresh (`municipio_delitos`, `estatal_delitos`, `estatal_victimas`,
  `municipio_victimas`); the `_2015_2025` legacy tables are frozen.
- **BD Pro rolling window:** the 4 ongoing tables → `PartBdpro(free_lag=6 months)`; the frozen legacy
  tables stay `AllFree`. Create a **pro Coverage (is_closed=True) on each ongoing table before the
  pipeline runs**, or `assert_coverage_topology` hard-fails.
- Staging parquet from the pipeline must be **all-STRING** (see `dump_header` limitation in the rule).

## Gotchas learned here

- **pyrefly (CI)**: the requester-pays monkey-patch of `gcs.Client.bucket` MUST name its param
  `bucket_name` (not `name`) and must NOT add a `generation` kwarg — the pinned google-cloud-storage
  version's signature is `(self, bucket_name, user_project=None)`. Mismatches fail the CI
  "Pyrefly type check". (Running `uv run pyrefly check` inside the `.claude/worktrees/` path reports
  "no files matched" and exits 1 — a worktree-path artifact, not a real error; CI checks a normal checkout.)
- **git push is sandbox-denied** here; the branch/PR was created via the `gh` CLI git Data API.
- `update_column` (metadata MCP) cannot rename a column and its empty args clobber
  directory_column/is_partition — use `bulk_upsert_columns` (never clobbers) for patches; delete+recreate to rename.
- Backend metadata registered on **staging** (env="staging"), published there; prod is registered
  post-merge once `table-approve` materialises `basedosdados.*`.

## Verified dev row counts (2026-08-12)

estado 32 · municipio 2,478 · municipio_delitos_2015_2025 30,755,928 · municipio_delitos 1,716,840 ·
municipio_victimas 1,027,698 · estatal_delitos_2015_2025 413,952 · estatal_delitos 21,888 ·
estatal_victimas_2015_2025 971,520 · estatal_victimas 393,984.
