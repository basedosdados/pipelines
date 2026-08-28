# br_mgi_compras_publicas — onboarding plan

Source: `https://dadosabertos.compras.gov.br` (API Compras.gov.br v1.0.0, OpenAPI at
`/v3/api-docs`). Organization: Ministério da Gestão e da Inovação em Serviços
Públicos (MGI). Licence: **CC BY 4.0**, declared by the API itself in the OCDS
module's `publicationPolicy`. All figures below measured 2026-08-28.

## 1. Coverage

### Temporal

| Module | Span | Rows |
|---|---|---|
| Contratações (Lei 14.133) | **2021 → 2026-07-23** | 931,568 headers |
| Contratação — itens | 2021 → 2026-07-23 | ~7.6M |
| Contratação — resultados (lances vencedores) | 2021 → 2026-07-23 | ~5.7M |
| Atas de registro de preço | 2023 → 2027 (vigência) | 385,066 |
| Atas — itens | 2023 → 2027 | ~2.5M |
| Contratos / contratos-itens | per órgão, 2021 → 2026-07 | ~1M / ~2M |
| **Legado (Lei 8.666)** | **1998 → 2025** | ~1M licitações |

Contratações by year: 2021 7,443 · 2022 39,822 · 2023 110,316 · 2024 296,158 ·
2025 321,302 · 2026 156,527 (part-year).

**The feed is stale.** No record of any kind is published after **2026-07-23** —
verified daily through 2026-08-28 on contratações, itens and resultados. The API
itself is healthy (11.6M requests in Aug 2026, 98.5% success per
`/modulo-indicadores/1_*`); this is an upstream warehouse-refresh stall. The poll
guard handles it correctly — scheduled runs no-op until the source moves again.

### Levels of government

Contrary to the "compras do governo federal" branding, the 14.133 module carries
**all three levels**. June 2025, 24,321 contratações:

| Esfera | Registros | Share | Órgãos distintos |
|---|---|---|---|
| F — Federal | 10,404 | 42.8% | 564 |
| E — Estadual | 7,607 | 31.3% | 403 |
| M — Municipal | 6,120 | 25.2% | 835 |
| N — Não classificado (estatais, consórcios, fundações, Sistema S) | 920 | 3.8% | 164 |
| D — Distrital | 162 | 0.7% | 31 |

All 27 UFs; 890 distinct municipalities in that single month (of 5,570). Powers:
Executivo 71%, Não-informado 21%, Judiciário 5%, Legislativo 3%.

Municipal coverage is therefore **partial** — only entities that transact through
the Compras.gov.br platform. Registries: 11,872 active órgãos, 21,970 active UASGs,
826,570 active suppliers.

### Federal organizations

Top federal buyers by contratação count (June 2025): Comando do Exército (2,191),
Comando da Marinha (1,614 across its CNPJs), Comando da Aeronáutica (342), TST
(305), TSE (304), Ministério da Saúde (276), MPU (247), Justiça Federal 1ª
Instância (225), Fiocruz (147). Defence is roughly a third of federal volume.
Largest state buyers are São Paulo's secretarias and USP/Unesp/Unicamp; largest
non-classified is EBSERH (344).

## 2. Redundancy — 77 endpoints collapse to 19 tables

### Dropped as redundant or empty

| Module | Verdict |
|---|---|
| **11 — OCDS** | The same contratações re-serialised to Open Contracting Data Standard, keyed by `buyerID`. Pure re-format. **Drop.** |
| **03 — Pesquisa de preço** | Keyed by catalogue item code, so harvesting means iterating 343,880 CATMAT codes. Its payload is a projection of `contratacao_item_resultado` (same `idCompra`, `idItemCompra`, preço, fornecedor). **Derivable — drop.** |
| **04 — PGC (planning / PCA)** | Returns 0 for every órgão × year tried (12 real órgãos, 2024–2026, both código and CNPJ forms). **Empty — drop**, and note that `br_pncp` deferred its own PCA table for cost reasons, so procurement planning stays uncovered. |
| **97 — Indicadores** | API telemetry (request counts, latency), not procurement. **Drop.** |
| **98/99/AUTENTICACAO** | ALICE integration, user management, login. **Drop.** |
| Catálogo hierarchy endpoints (grupo, classe, PDM, seção, divisão, subclasse) | Every level is already denormalised onto the item row (`codigoGrupo`, `nomeGrupo`, `codigoClasse`, …). **Fold into the item tables** rather than shipping 11 near-empty dimension tables. |
| ARP sub-endpoints 3/4/5 (unidades participantes, empenhos, adesões) | Require `numeroAta` + `unidadeGerenciadora` + `numeroItem` per call — 385k atas × N items. **Defer**; revisit if adesões specifically are wanted. |

### Overlap with `br_pncp` (in flight) — the important one

The 14.133 module is a **20% subset of PNCP, not a mirror**. Same four modalities,
June 2025: compras.gov.br **24,321** vs PNCP **119,082**. It covers only what
transacts through the Compras.gov.br platform.

| Table | Overlaps `br_pncp`? | Keep? |
|---|---|---|
| `contratacao` | Yes — subset of `br_pncp.contratacao` | **Keep.** It is the parent row for the items, and carries SIASG keys PNCP lacks (`idCompra`, `codigoOrgao`, `codigoUasg`, SIASG `codigoModalidade`) — the bridge to the legado/SIASG world and to `contrato`. |
| `contratacao_item` | **No** | Keep — the headline asset |
| `contratacao_item_resultado` | **No** | Keep — the headline asset |
| `ata_registro_preco` | Yes — subset | Keep (parent of the items) |
| `ata_registro_preco_item` | **No** | Keep |
| `contrato` | Yes — subset | Keep (parent) |
| `contrato_item` | **No** | Keep |

**PNCP's consulta API serves items only one contratação at a time**; compras.gov.br
serves them in bulk by date window. That is the entire reason this dataset earns its
place next to `br_pncp`, and it should be said plainly in the dataset description.

## 3. Table list

Superseded by section 7, which is the authoritative list once legado was folded
in. Directory foreign keys are unchanged: `id_municipio` →
`br_bd_diretorios_brasil.municipio` (via `unidadeOrgaoCodigoIbge`), `sigla_uf` →
`.uf`, `ano` → `br_bd_diretorios_data_tempo.ano`, and supplier `cnpj` links to
`br_rf_cnpj`.

## 4. API constraints that shape the harvest

1. **Date ranges are half-open `[inicial, final)`.** `inicial == final` returns
   `totalRegistros: 0` under HTTP 200 — a silent trap. Adjacent windows tile exactly.
   A "monthly" window must end on the 1st of the next month.
2. **Max window 365 days**, hard 400. `2024-01-01..2025-01-01` is 366 days and fails.
3. **`tamanhoPagina` is 10–500**; out of range is a 400 with a plain-text body.
4. **Latency grows with pagination depth** (~1.5s shallow, ~5s at page 300/500).
   Prefer narrow windows over deep paging. 4 concurrent workers were stable.
5. **`codigoModalidade` is required on the contratação header and uses SIASG codes.**
   Only 4 carry data: 3 Concorrência Eletrônica, 5 Pregão Eletrônico, 6 Dispensa,
   7 Inexigibilidade. A full harvest loops those 4 per window.
6. **`contrato` and `contrato_item` require `codigoOrgao`** — 11,872 órgãos must be
   iterated. No date-only entry point exists.

## 5. Revision behaviour — how far back a run must look

Measured on 4,000-row samples, lag between `dataInclusaoPncp` and
`dataAtualizacaoPncp`:

| Table | Same day | ≤30d | ≤180d | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|---|
| `contratacao` | 97.4% | 99.6% | 99.9% | 0d | 0d | 7d | 362d |
| `contratacao_item` | **1.6%** | 22.6% | 98.9% | **78d** | **106d** | 192d | 395d |
| `contratacao_item_resultado` | 99.0% | 99.3% | 100% | 0d | 0d | 0d | 190d |

**Your intuition is right, and it is concentrated entirely in the item table.**
Headers and results are written once and essentially never revised; **items are
filled in progressively over about three months** — median 78 days after inclusion.
Independently: of the 3,006,944 items included in 2025, **399,153 (13.3%) were last
updated in 2026** — a publication-date-only harvest would carry stale values on
1 row in 8.

An incremental `since` filter exists but works on **only one of the three endpoints**:

| Endpoint | Update filter | Behaviour |
|---|---|---|
| `2_consultarItensContratacoes` | `dataAtualizacaoPncp` | **Works** — `>= date`, monotonic. Verified across 8 dates. |
| `1_consultarContratacoes` | `dataAualizacaoPncp` (sic — typo'd in the spec) | **Broken.** The documented spelling is ignored (returns the unfiltered set); the correct spelling returns 0 for every date. |
| `3_consultarResultadoItens` | `dataAtualizacaoPncp` | **Ignored** — returns the unfiltered count for every date. |

Which is convenient: the one table that genuinely needs incremental refresh is the
one that supports it.

### Recommended daily schedule

| Table | Strategy | Cost/run |
|---|---|---|
| `contratacao_item` | `dataAtualizacaoPncp >= last_run - 2d`, looped over year windows | minutes |
| `contratacao` | re-pull trailing **30 days** (99.6% settled) | ~3 min |
| `contratacao_item_resultado` | re-pull trailing **30 days** (99.3% settled) | ~25 min |
| `ata_*` | re-pull trailing 90 days of vigência | ~10 min |
| `contrato*` | **weekly**, not daily — 11,872 órgãos ≈ 2h | — |
| registries + catálogos | weekly snapshot | ~1h |
| all fact tables | **quarterly full rebuild** to sweep the long tail (>180d revisions, 0.1–1%) | — |

Backfill estimate: **25–35h** at 4 workers, resumable per window — comparable to
`br_pncp`'s 40h.

## 6. Legado (Lei 8.666) — included, and it is half the dataset

Decisions taken 2026-08-28: headers ship despite the `br_pncp` overlap; legado is in
scope now, not a phase 2; BD Pro is `PartBdpro(free_lag=6 months)` on fact tables and
`AllFree` on registries and catálogos; the `dicionario` is built from the code/name
pairs the payloads already carry.

### Coverage — 1997 to 2025

| Endpoint | Table | Rows | Span |
|---|---|---|---|
| `1_consultarLicitacao` | `licitacao` | 1,018,866 | 1997–2025 |
| `3_consultarPregoes` | `licitacao_pregao` | 1,107,795 | 2000–2023 |
| `2_consultarItemLicitacao` | `licitacao_item` | **53,906,552** | — |
| `4_consultarItensPregoes` | `licitacao_item_pregao` | 26,856,321 | 2000–2025 |
| `5_consultarComprasSemLicitacao` | `compra_sem_licitacao` | 5,235,901 | 1997–2024 |
| `6_consultarCompraItensSemLicitacao` | `compra_sem_licitacao_item` | 15,738,108 | 1997–2024 |
| `7_consultarRdc` | — | 6,360 | dropped, see below |

Licitações split 487,737 pre-2013 / 531,129 from 2013; direct contracting splits
3,591,067 pre-2013 / 1,644,834 from 2013. Direct contracting peaks in 2002 at
342,895 in one year and declines steadily thereafter.

### Overlap with `br_cgu_licitacao_contrato`

That dataset already holds federal licitações for **2013–2024** (1,742,814 headers,
14.5M itens, 75.5M participantes, 7.4M empenhos), and its `licitacao` table folds in
Dispensa (1,118,827) and Inexigibilidade (233,685) — i.e. it merges what this API
splits across `licitacao` and `compra_sem_licitacao`.

So legado's unique contribution is **1997–2012 — fifteen years, ~4.08M headers** that
`br_cgu_licitacao_contrato` cannot see, plus the 2025 tail and the pregão process
fields (portaria, adjudicação, homologação, responsáveis) CGU lacks. Conversely CGU
keeps `licitacao_participante` and `licitacao_empenho`, which this API does not expose
at all — the two datasets are complements, and both descriptions should say so.

Harvest the full 1997–2025 range rather than only pre-2013: a single-source
continuous series is worth more than a stitched one, and the marginal cost of the
overlapping years is small next to the pre-2013 backfill.

### `7_consultarRdc` is dropped

RDC is modalidade 99 and already appears inside `licitacao` (30 rows in June 2015
alone; 6,360 total across all years). The endpoint adds only
`forma_de_realizacao_licitacao`, `orgao_uasg` and `uf_uasg`. Not worth a table.

### The one expensive endpoint

`2_consultarItemLicitacao` has **no date filter of any kind** — the only partition key
is `modalidade` (required) and optionally `uasg`. Measured throughput is
**178 rows/s at 8 workers** (12 workers is slower: 148). Pagination latency is flat
(page 1 = 12.5s, page 5000 = 14.5s), so there is no OFFSET rescan to design around —
it is simply slow. 53.9M rows ⇒ **~84 hours**.

The date-partitioned endpoints are 4.5–5.5× faster per row, measured at 8 workers:

| Endpoint | rows/s | Rows | Backfill |
|---|---|---|---|
| `1_consultarLicitacao` | 1,395 | 1.02M | 0.2h |
| `5_consultarComprasSemLicitacao` | 914 | 5.24M | 1.6h |
| `6_consultarCompraItensSemLicitacao` | 977 | 15.74M | 4.5h |
| `4_consultarItensPregoes` | 804 | 26.86M | 9.3h |
| **`2_consultarItemLicitacao`** | **178** | **53.91M** | **84h** |

`4` and `6` together cover 42.6M of endpoint 2's 53.9M rows, but **they are not
substitutes**. Endpoint 4 is keyed on homologation date, so items never homologated —
cancelled, deserted, fracassado lots — have no `dt_hom` and never appear: 26.86M vs
endpoint 2's 32.40M for modalidade 5, a **17% shortfall biased exactly toward failed
procurement**. Endpoint 6 is 15.74M vs endpoint 2's 16.71M for modalidades 6+7, a 6%
shortfall. Six modalidades (Convite 3.50M, Tomada de Preços 898k, Concorrência 372k,
Concorrência Internacional 7.4k, Concurso 5.7k, RDC 15.2k = **4.80M rows**) exist in
endpoint 2 *only*.

**Plan: run in two tiers.** Legado is a closed archive — Lei 8.666 procurement has
ended and the 2025 tail is 7,562 rows — so tier B can run whenever, without redoing
tier A.

- **Tier A (~22h, ships first):** endpoints 1, 3, 4, 5, 6 in full, plus endpoint 2
  restricted to modalidades 1, 2, 3, 4, 20, 99 (4.80M, 7.5h). Yields 47.4M item rows.
  Documented gap: ~6.5M non-homologated pregão and dispensa items.
- **Tier B (+77h, background):** endpoint 2 for modalidades 5, 6, 7, closing the gap.
  Until it lands, `licitacao_item` carries a `observations` note naming the omission.

### Legado date ranges are CLOSED, not half-open

The 14.133 module uses `[inicial, final)`; **legado uses `[inicial, final]`**. Verified:
`2015-06-08` alone = 142, `06-09` = 180, `06-10` = 208, and `[06-08..06-10]` = 530 =
their sum. A shared windowing helper across the two modules would double-count on one
side or drop a day on the other. They need separate window generators.

`id_compra` is the primary key of `licitacao` and is unique (3,820 rows, 3,820 distinct
in June 2015). `licitacao_pregao` reports 1,107,795 rows against `licitacao`'s
1,018,866 — pregões whose `data_publicacao` is null never surface in any `licitacao`
date window, so the pregão table is the more complete header for that modalidade.
Flag this in `observations` rather than silently reconciling.

## 7. Full table list (19)

**14.133 facts** (partition `ano`, `PartBdpro` free_lag 6 months)
1. `contratacao` · 932k · 2021–2026
2. `contratacao_item` · ~7.6M
3. `contratacao_item_resultado` · ~5.7M
4. `ata_registro_preco` · 385k · 2023–2027
5. `ata_registro_preco_item` · ~2.5M
6. `contrato` · ~1M
7. `contrato_item` · ~2M

**Legado facts** (partition `ano`, `AllFree` — closed archive, no rolling window)
8. `licitacao` · 1.02M · 1997–2025
9. `licitacao_pregao` · 1.11M · 2000–2023
10. `licitacao_item` · 47.4M tier A / 53.9M tier B
11. `licitacao_item_pregao` · 26.9M · 2000–2025
12. `compra_sem_licitacao` · 5.24M · 1997–2024
13. `compra_sem_licitacao_item` · 15.7M · 1997–2024

**Registries and catálogos** (partition `data_extracao`, `AllFree`)
14. `orgao` · 11,872 active
15. `unidade_administrativa` (UASG) · 21,970 active
16. `fornecedor` · 826,570
17. `catalogo_material` (CATMAT) · 343,880
18. `catalogo_servico` (CATSER) · 3,096
19. `dicionario`

### `dicionario` contents

Built from the code/name pairs the payloads already carry — no transcription needed,
and it stays correct as the source adds values. Covered: `modalidade` (both the SIASG
and the PNCP code sets, since records carry `codigoModalidade` **and**
`modalidadeIdPncp` — 6 ↔ 8 for Dispensa), `amparo_legal`, `situacao_compra`,
`situacao_compra_item`, `modo_disputa`, `criterio_julgamento`, `tipo_beneficio`,
`tipo_instrumento_convocatorio`, `esfera` (F/E/M/D/N), `poder` (E/J/L/N),
`porte_fornecedor`, `natureza_juridica`, `tipo_item` (M/S), `orcamento_sigiloso`.
Legado adds its own `modalidade` set (1 Convite, 2 Tomada de Preços, 3 Concorrência,
4 Concorrência Internacional, 5 Pregão, 6 Dispensa, 7 Inexigibilidade, 20 Concurso,
99 RDC) — keyed separately from the 14.133 set, which reuses the same integers for
different things.

## 8. Total cost — revised after measuring the rate limiter

The API rate-limits with HTTP 429 (no `Retry-After` header; the cooldown is named in
the body), and **the ceiling differs about sevenfold by module**:

| Module | Converged rate |
|---|---|
| `modulo-legado`, `modulo-uasg`, `modulo-material` | ~4 req/s |
| **`modulo-contratos`** | **~0.6 req/s** |

The client paces each module with its own AIMD limiter, and a 429 does not consume
the retry budget reserved for genuine transient errors — otherwise the slow module
aborts the whole harvest.

### The contrato loop was the problem

`/modulo-contratos/` requires `codigoOrgao`, has no date-only entry point, and is
still bound by the universal 365-day window cap. The naive loop is 11,872 orgaos x
16 years = ~190k requests, which at 0.6 req/s is **89.5 hours**.

Measured on a random 90-orgao sample, **only 6% of registered orgaos hold any
contract** (5 of 90; all five had 2024 contracts and none were history-only). So the
loop probes each orgao once on the densest year and expands only the hits, unioned
with orgaos already visible in harvested procurement — every contract originates in
some compra, and the harvest covers all procurement from 1997, so that second source
is free. That turns ~190k requests into roughly 12k-24k.

| Phase | Hours |
|---|---|
| Registries and catalogues | ~1 |
| 14.133 contratacao, itens, resultados, atas | ~6 |
| Legado tier A excluding endpoint 2 | ~14 |
| Legado endpoint 2, tier A modalidades (4.8M rows, server-bound at 178 rows/s) | ~8 |
| **contrato + contrato_item** (probe + expansion, both on the 0.6 req/s module) | **~12-24** |
| **Backfill subtotal** | **~41-53** |
| Legado tier B (optional, background) | +77 |

### Disk

Measured 82-330 bytes/row of all-STRING snappy parquet, so tier A is about **10 GB of
chunks and 10 GB consolidated**. The machine had 31 GiB free at 97% utilisation when
this ran, which fits but is not comfortable; `--prune-chunks` drops each table's
chunks once consolidated and holds the peak near 11 GB.

## 9. Recurring pipeline

Daily, per section 5. Legado needs no daily refresh — an annual re-pull is enough
(records do carry a live `dt_alteracao`; 2015 rows were last touched 2025-04-24).
The 14.133 poll guard will no-op until the source clears its 2026-07-23 stall.

## 10. Auxiliary files: none

The source publishes data and a machine-readable OpenAPI spec, and nothing else.
`dadosabertos.compras.gov.br` redirects straight to the Swagger UI, and the
gov.br dados-abertos page for Compras is a 404. There is no codebook,
questionnaire or technical manual to bundle.

The spec is genuine documentation, but it is dataset-level rather than
table-level, and it is already reachable from the raw data source URL registered
in the backend. Copying it into nineteen identical per-table bundles would add
noise, not access. Step 6b is therefore skipped deliberately, not overlooked.

The coded columns are documented instead through the `dicionario` table, built
from the code/name pairs the API's own payloads carry.
