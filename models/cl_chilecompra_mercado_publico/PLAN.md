# cl_chilecompra_mercado_publico — onboarding plan

**Source:** ChileCompra / Mercado Público (Dirección de Compras y Contratación Pública, Ministerio de Hacienda, Chile)
**Scope:** all Chilean public procurement, item level, 2007-1 → 2026-2 (semester files)

## 1. Source verification (done)

**The files are MONTHLY, not semestral.** The brief assumed `YYYY-{1,2}` were semester
files; they are `YYYY-{1..12}`. Verified two ways: `2025-13` returns 404 while `2025-3`,
`2025-6`, `2025-7`, `2025-9`, `2025-12` all return 200, and every one of the 349,623 rows
in `oc-da/2026-1.zip` has `FechaEnvio` inside 2026-01-01 … 2026-01-31. The publisher's
Descargas page agrees: *"La cantidad de información **mensual** puede variar"*.

Consequence: **480 files, not 40.** Onboarding only months 1 and 2 would have silently
dropped ten twelfths of the data while looking complete.

| Container | URL pattern | Files | Compressed each | Uncompressed each |
|---|---|---|---|---|
| órdenes de compra | `https://transparenciachc.blob.core.windows.net/oc-da/YYYY-M.zip` | 240 (2007-1 … 2026-12) | 37–105 MB | 328 MB (2007-01) – 588 MB (2026-01) |
| licitaciones | `https://transparenciachc.blob.core.windows.net/lic-da/YYYY-M.zip` | 240 | 12–47 MB | 233 MB (2026-01) |

`2006-*`, `2027-*` and `YYYY-13` return 404 — coverage boundary confirmed. Each ZIP holds
exactly one CSV (`YYYY-M.csv` for OC, `lic_YYYY-M.csv` for LIC).

Refresh cadence, per the publisher's own Descargas page: the *Descarga Masiva* files
(these) are rebuilt **daily between 12:00 and 14:00 (Chile time), with one day of lag**,
covering 2007→present.

**Old months are rewritten retroactively.** `oc-da/2026-1.zip` (January) carried a
Last-Modified of 2026-08-25, and the licitaciones files for 2024 were rewritten in April
2026. So "freeze closed periods, only refresh the open one" is unsafe. The pipeline must
key off each blob's `Last-Modified`/`ETag` and re-ingest exactly the months whose blob
changed — a 480-request HEAD sweep is cheap and is the only correct trigger.

## 2. Parsing traps found (all verified against real bytes)

1. **Encoding is Windows-1252, not Latin-1 nor UTF-8.** Every file fails UTF-8 decode.
   Non-ASCII bytes include `0x92 0x93 0x94 0x96 0x97` (smart quotes, dashes), which are
   *control characters* in Latin-1. Some files also carry bytes undefined in cp1252
   (`0x81`), so decoding needs `encoding="cp1252", encoding_errors="replace"`.
2. **Decimal separator is a comma**, field separator is `;`. Real values: `892,5`,
   `76924550,390625`. Naïve numeric parsing silently produces nulls or 892500.
3. **Quoted fields contain embedded newlines and embedded `;`** (e.g. `CriteriosEvaluacion`
   = `"Cumplimiento Programa de Integridad ; Presentación de Antecedentes"`). Line-based
   splitting is not safe; a real CSV parser is required.
4. **Null sentinels are heterogeneous:** unquoted `NA`, empty `""`, single space `" "`,
   and `1900-01-01` for dates. All must map to NULL.
5. **Trailing whitespace in categorical values:** `"Región del Maule "`.
6. **Schema drift, same column count.** OC 2007 and OC 2026 both have 78 columns but are
   *different* columns — `idPlanDeCompra` (2007–2020) vs `Codigo_ConvenioMarco` (2021+).
   Positional parsing would silently misalign two decades of data. Parse by header name.
7. **Header accents were deleted in later eras.** 2007–2014 LIC headers carry proper
   accents (`Nombre producto genérico`); 2015+ headers have the accented character
   *dropped* (`Nombre producto genrico`, `Tipo de Adquisicion`). Accent-stripping alone
   does not reconcile them — an explicit alias table is required.
8. **The publisher's own header has a duplicated name**, exported as
   `DescripcionCriteriosRequisitosSociales` and `...Sociales.1`.

### Header signatures

| Table | Files | Signatures | Union | Notes |
|---|---|---|---|---|
| OC | 40 | 3 (78 / 77 / 78 cols) | 79 | 2017 lacks both `idPlanDeCompra` and `Codigo_ConvenioMarco` |
| LIC | 40 | 3 (106 / 105 / 110 cols) | 118 raw → ~112 logical after alias mapping | criterios ambientales/sociales added 2020+ |

## 3. Grain (measured on 2026-1)

**Licitaciones** — 118,536 rows = 7,198 tenders × items × bids. One row per
**tender × item line × bidding supplier**, i.e. the file includes every offer received,
not only the winner.

| Candidate key | Duplicate rows |
|---|---|
| `CodigoExterno` | 93.9 % |
| `+ Codigoitem` | 73.1 % |
| `+ CodigoProveedor` | 0.5 % |
| `+ Nombre de la Oferta` | 0.2 % |

142 exact full-row duplicates. So the natural key is
`(CodigoExterno, Codigoitem, CodigoProveedor, Nombre de la Oferta)` with a residual
~0.2 % that needs `custom_unique_combinations_of_columns` with an allowed-failure
proportion, documented in the model description.

**Órdenes de compra** — `(Codigo, IDItem)` is an exact key: **0 duplicates** across all
349,623 rows of 2026-01 (131,850 distinct orders, so ~2.7 item lines per order). No
full-row duplicates. Currency is overwhelmingly CLP (347,467) with CLF/UTM (inflation-
indexed units), USD and EUR present — so a currency column plus the publisher's own
`MontoTotalOC_PesosChilenos` conversion are both needed.

**`CriteriosEvaluacion` is order-unstable.** The same tender appears twice with the same
evaluation criteria listed in a different order (`"Cumplimiento…; Presentación…; Oferta…"`
vs `"Oferta…; Presentación…; Cumplimiento…"`). Normalise by splitting on `;`, trimming and
sorting the tokens before de-duplicating, or the residual duplicate rate is inflated.

## 4. Data caveat that must reach the table description

ChileCompra states the bulk OC file deliberately **includes purchase orders it excludes
from its own official statistics** (those with errors in amounts or currency type).
Aggregates computed from this table will therefore not reconcile with ChileCompra's
published figures. This belongs in the table description and observations, not only here.

## 5. Open decisions — see checkpoint

License, table split, geography FK targets, and BD Pro tiering.

## 6. Measured scale (full HEAD sweep of all 480 candidate URLs)

**472 files exist, 2007-01 → 2026-08, 26.96 GB compressed.** The only 404s are
2026-09 … 2026-12, i.e. months that have not happened yet. No gaps anywhere in the
19.7-year history.

| Container | Files | Compressed | Est. uncompressed | Est. rows |
|---|---|---|---|---|
| `oc-da` | 236 | 19.01 GB | ~139 GB (7.3x observed) | ~83 M item lines |
| `lic-da` | 236 | 7.94 GB | ~132 GB (16.6x observed) | ~67 M bid lines |
| **total** | **472** | **26.96 GB** | **~270 GB** | **~150 M** |

That is ~3x the row count and ~7x the uncompressed volume assumed in the brief, because
the brief treated the files as semestral. Compressed volume per year is flat at
1.0-1.6 GB, so the cost is spread evenly rather than concentrated in recent years.

Consequence for the build: download, clean, write parquet, then **delete the raw month**,
one month at a time, so peak disk stays near a single file (~600 MB) rather than 270 GB.

## 7. Partition keys (verified, not assumed)

| Table | Partition | Evidence |
|---|---|---|
| `orden_compra_item` | `ano`, `mes` from `FechaEnvio` | all 349,623 rows of `oc-da/2026-1` fall in 2026-01-01..2026-01-31 |
| `licitacion_item` / `licitacion_oferta` | `ano`, `mes` from `FechaPublicacion` | 100% of `lic-da/2026-1` rows are in 2026-01; `FechaCreacion` is not (24% fall in prior months) |

## 8. Table design (decided)

| Table | Grain | Key |
|---|---|---|
| `orden_compra_item` | purchase order x item line | `(codigo_orden_compra, id_item)` - exact, 0 dups |
| `licitacion_item` | tender x item line | `(codigo_externo, codigo_item)` |
| `licitacion_oferta` | tender x item x bidding supplier | `(codigo_externo, codigo_item, codigo_proveedor, nombre_oferta)` |
| `dicionario` | coded value to label | from the publisher's Definiciones page |

Splitting `licitacion` avoids repeating ~62 tender-level columns on every one of the ~16
bid rows per tender. `orden_compra_item.codigo_licitacion` joins to the licitacion tables.
