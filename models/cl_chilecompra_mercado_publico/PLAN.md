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

### Header signatures (all 472 files, not a sample)

An initial survey of months 1 and 2 of each year found three signatures per table and
put the boundaries in the wrong places. Sweeping every one of the 472 files gives four
signatures per table, with boundaries that never fall in January:

**Órdenes de compra**

| Sig | Cols | Files | Months |
|---|---|---|---|
| 0 | 78 | 155 | 2007-01..2016-09 **and** 2017-08..2020-09 |
| 1 | 77 | 3 | 2016-10..2016-12 |
| 2 | 77 | 7 | 2017-01..2017-07 |
| 3 | 78 | 71 | 2020-10..2026-08 |

Note sig0 *returns* after a 22-month absence, and sig1 and sig2 have the same column
count while differing in content. The transitions are: `Link` dropped 2016-10, restored
2017-01 while `idPlanDeCompra` was dropped; `idPlanDeCompra` restored 2017-08; and
`idPlanDeCompra` finally replaced by `Codigo_ConvenioMarco` in 2020-10.

**Licitaciones**

| Sig | Cols | Files | Months |
|---|---|---|---|
| 0 | 106 | 86 | 2007-01..2014-02 |
| 1 | 111 | 2 | 2014-03..2014-04 |
| 2 | 105 | 67 | 2014-05..2019-11 |
| 3 | 110 | 81 | 2019-12..2026-08 |

2019-12 drops `ValorTiempoRenovacion` and adds the six environmental/social criteria
columns.

Because the parser matches on header name rather than position, none of these boundaries
need to be encoded anywhere. All 472 files were checked against the architecture plus the
explicit exclusion list: zero unknown columns.

### The 2014-03/2014-04 personal-data columns

Signature 1 exists in exactly two files and carries nine columns naming individual
public officials -- `RutUsuario`, `CodigoUsuario`, `NombreUsuario`, `CargoUsuario`,
`NombreResponsablePago`, `EmailResponsablePago`, `NombreResponsableContrato`,
`EmailResponsableContrato`, `FonoResponsableContrato`. They are fully populated there
(about 4,000 distinct people across 1.5M rows) and absent from the other 234 files.

They are **deliberately excluded**: including them would publish names, job titles,
emails and telephone numbers that the rest of the procurement record does not, in a
column that would be 99% null. Chile's Ley 19.628 governs this data and ChileCompra's
own terms invoke it. The exclusion is an explicit constant, and any source column that
is neither in an architecture table nor in that constant now raises rather than being
quietly dropped.

### A column that looked dead but is not

`idPlanDeCompra` is null in 100% of rows in 2007-01 and 2010-06, and in all but 10 of
505,159 rows in 2015-06 -- which reads like a dead column worth dropping. It is not: in
2019-06 and 2020-06 it is populated in about 41% of rows with 865 distinct values. It is
kept, and the architecture records exactly this.

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

## 9. Geography: linking to br_bd_diretorios_cl

ChileCompra publishes región and comuna as free text and never as a código único
territorial, so the directory link is a name lookup. It lives in a checked-in crosswalk
(`code/geografia_crosswalk.csv`, built by `code/build_geografia_crosswalk.py`) rather
than fuzzy matching at load time: fuzzy matching quietly returns a different answer as
the data drifts, while a table returns the same answer or none.

Five derived columns carry the FK: `id_region_unidad_compra` and `id_comuna_unidad_compra`
on both `orden_compra_item` and `licitacion_item`, plus `id_region_proveedor` and
`id_comuna_proveedor` on `orden_compra_item`.

**Buyer and supplier geography are not the same kind of field.** Buyer geography comes
controlled by the system and resolves to a CUT for 100% of non-null rows. Supplier
geography is self-reported free text and resolves for about 97%, with an irreducible
tail — `CUARTA REGION`, `DECIMA LOS LAGOS`, `Santiago, Maipu`, `Extranjero`. The id
columns are best-effort and each says so; the verbatim published value is always kept in
the adjacent name column. This only shows up if you test a year after 2010: the 2007
months sampled first have clean supplier values throughout.

Four classes of mismatch had to be handled, all found by diffing real values against the
directory rather than guessed:

| Problem | Example |
|---|---|
| Source truncates at 35 characters | `Región Aysén del General Carlos Iba` |
| Acute accent U+00B4 where an apostrophe belongs | `...Bernardo O´Higgins` |
| Alternative names | `Puerto Natales` for `Natales` |
| Orthographic variants | `Llay-Llay`/`Llaillay`, `Til Til`/`Tiltil`, `La Calera`/`Calera` |

One trap worth stating plainly: **`"region de la "` must not be stripped as a prefix.**
Several directory names begin with their article (`La Araucanía`, `Los Lagos`), so
stripping it turns `Región de la Araucanía` into `araucania` and misses `la araucania`.
Only `region del `, `region de ` and `region ` are stripped.

Comuna names were verified unique nationally (346 distinct of 346) before relying on a
name lookup at all. `Arica 1` is deliberately left unresolved: it is a branch designator
leaking into the comuna field, not a comuna.

## 10. Two performance traps hit during the load

**Whole-file parsing would have OOMed.** `lic-da/2014-3` is 1.5M rows across 111 columns;
held at once as pandas object strings that is roughly 11 GB, which does not fit on a
16 GB machine. Parsing in 200k-row chunks and projecting each chunk onto the output
tables first drops the measured peak on that file to 1.00 GB. Verified byte-identical to
the whole-file path on both kinds, with frame equality rather than row counts. De-duplication
runs again after concatenating, since a duplicate pair straddling a chunk boundary would
otherwise survive.

**Folding geography per row cost 5x.** `resolve_geography` originally ran three chained
`.map()` calls over every row, each boxing the element and calling `pd.isna` plus
`unicodedata.normalize`. A column of 350k rows holds a few hundred distinct place names,
so the work was O(rows) where O(distinct) suffices. One month went from 83s to 419s;
folding uniques and applying a single `.map(dict)` restored it to 80s.

## 11. Verification status

| Check | Result |
|---|---|
| Header coverage | all 472 files, zero unknown columns |
| Key uniqueness | 0 duplicates on all three tables |
| Chunked vs whole-file parity | frame-equal on both kinds |
| Geography resolution | región 14/14, 13/13, 14/14; comuna 342/342, 105/106 |
| dbt parse | 4 models, 27 tests, 5 relationships resolving to the real directory |
| Unit tests | 20 passing, no network |
| pyrefly | 0 diagnostics (3 suppressed, the standard Prefect trio) |

Not yet done: BigQuery upload, dbt run/test against real data, metadata registration,
auxiliary-file upload, PR.

## 12. Source defects found during the full load

Three months behave unlike the other 469, and each would have corrupted the load
silently rather than failing loudly.

### April 2014 licitaciones do not exist

`lic-da/2014-4.zip` contains a file named `2014-4.csv` whose 1,532,452 rows are **100%
March 2014** — the same 20,511 tenders and the same first tender as `lic-da/2014-3.zip`,
the same byte length (91,558,880), uploaded 15 seconds apart. The publisher put March's
data in April's slot.

This is a genuine gap in the series and must be stated in the table description. It is
not a gap in this load.

It is also how a latent bug surfaced. Partition files were written as a fixed
`data.parquet` keyed on the *derived* date, so loading April wrote into the March
partition and replaced it. Nothing was lost, because the two files hold identical data,
but the mechanism is general.

### One row of `lic-da/2026-3` carries a June date

Corruption residue: the tender's other 142 rows are March, and June's own file does not
contain that tender at all. With a fixed `data.parquet` name, loading March after June
would have replaced June's 160,682 rows with that single row.

**Partition files are therefore named for their source month** (`data_2026-03.parquet`),
so two source months can contribute to one partition without destroying each other, and
re-running a month overwrites exactly its own contribution. This matters more for the
recurring pipeline than for the one-shot load, because the pipeline re-ingests an
arbitrary rolling window in whatever order the publisher touched months.

The June 2026 partition now correctly holds both files:

```
data_2026-06.parquet   160,682 rows
data_2026-03.parquet         1 row
```

### `lic-da/2011-3` is split across two CSVs

`lic_2011-3a.csv` and `lic_2011-3b.csv`, 1.27 GB combined — the largest month — with
byte-identical headers. All CSV members of an archive are read in name order.

### `lic-da/2026-3` has an unescaped quote

It misaligns 849 records on one tender, giving field counts of 100, 111, 127 and 140
against a 110-column header. The C parser rejects the whole file, so `clean_month` falls
back to Python's `csv` module, which drops records whose width does not match and
reports the count.

Worth knowing: pandas would **not** have dropped the short ones. Its C parser pads a
too-short record with NaN, quietly writing shifted values into the right-hand columns.
Only records with too many fields raise. Hence the explicit width check.

## 13. Final load

| Table | Rows | Source months |
|---|---|---|
| `orden_compra_item` | 105,389,728 | 236/236 |
| `licitacion_oferta` | 96,611,435 | 235/236 |
| `licitacion_item` | 20,551,136 | 235/236 |
| **total** | **222,552,299** | |

Counted from the parquet footers, not `clean_log.jsonl` — that log is append-only across
runs, so any month re-run under `--force` appears more than once and summing it
over-counts. One schema per table, verified across all 707 files.

The dicionario has **164 entries**, up from the 115 derivable from 2007 alone: codes
appear over time (`codigo_tipo` 10 → 17 keys, `codigo_forma_pago` 34 → 44, licitación
`sigla_tipo` 5 → 14). Every dictionary-flagged column has entries.
