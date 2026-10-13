# br_mapbiomas_estatisticas — validation record

MapBiomas Collection 11 (30 m, 1985–2025), released 2026-08-12. Sources:

| File | Feeds |
|---|---|
| `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE_MUNICIPALITY.xlsx` (Google Drive, 75 MB zipped) | `cobertura_municipio_classe`, `cobertura_uf_classe`, `classe` |
| `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx` | `transicao_uf_de_para_{anual,quinquenal,decenal}`, and the state-level cross-check below |
| `legend_code_mapbiomas_brazil_collection_11.csv` | class colours |

## 1. Municipality codes against the Data Basis directory

`geocode` is the 7-digit IBGE code in every row; no malformed values.

| | count |
|---|---|
| Codes in both MapBiomas and `br_bd_diretorios_brasil.municipio` | 5,570 |
| In MapBiomas, not in the directory | 2 |
| In the directory, not in MapBiomas | 1 |

Nothing was dropped. The three exceptions are understood:

- **4300001 Lagoa Mirim** and **4300002 Lagoa dos Patos** (RS) — coastal lagoons
  that IBGE assigns codes to in the municipal mesh but which are not
  municipalities, so they are absent from the directory. Kept in the table;
  excluded from the `relationships` test only.
- **2605459 Fernando de Noronha** (PE) — the oceanic island, outside the
  continental extent MapBiomas maps. Absent from the data.

## 2. Municipal area against IBGE's official areas

`br_bd_diretorios_brasil.municipio` carries no area column, so the reference is
IBGE's official *Áreas Territoriais 2025*
(`AR_BR_RG_UF_RGINT_RGI_MUN_2025`, computed from the Malha Municipal Digital),
joined on the directory's `id_municipio`. All 5,572 MapBiomas codes have an
official area.

The MapBiomas footprint per municipality is **identical across all 41 years**
(max drift 0.000000%), as it must be — the raster is clipped to fixed boundaries.

Relative discrepancy `(MapBiomas − IBGE) / IBGE`, in %, over 5,572 municipalities:

| p0 | p1 | p5 | p10 | p25 | p50 | p75 | p90 | p95 | p99 | p100 |
|---|---|---|---|---|---|---|---|---|---|---|
| −34.96 | −1.52 | −0.29 | −0.05 | −0.004 | −0.000 | +0.003 | +0.011 | +0.066 | +0.88 | +10.27 |

mean −0.062, sd 1.006.

| threshold | municipalities | share |
|---|---|---|
| \|disc\| > 0.1% | 702 | 12.60% |
| \|disc\| > 0.5% | 276 | 4.95% |
| \|disc\| > 1.0% | 137 | 2.46% |
| \|disc\| > 5.0% | 20 | 0.36% |

National total 2025: MapBiomas 850,704,918 ha vs IBGE 850,936,086 ha, **−0.027%**.

**Reading.** The median municipality matches to five decimal places and the
interquartile range is ±0.004%. The tail has two identifiable causes:

- *Coastal water.* The five largest negatives are all on Baía Norte/Sul around
  Florianópolis — Florianópolis (−34.96%), Governador Celso Ramos (−26.51%),
  São José (−24.01%), Palhoça (−17.01%), Biguaçu (−13.42%). IBGE's legal area
  includes the enclosed bay; the MapBiomas raster does not.
- *Boundary vintage.* The remaining outliers cluster in Piauí and Maranhão on
  both sides (Arraial +10.27%, Acauã +7.02%, Sambaíba −17.88%, São José do
  Divino −18.35%), which is where IBGE's municipal limits have been revised.
  Small municipalities also show larger relative error simply because a 30 m
  pixel is a bigger share of them (Águas de São Pedro, 3.4 km², +6.72%).

No correction is applied. `area` is the raster footprint clipped to municipal
boundaries, not the legal area, and the column description says so.

**Not Observed** (class 0) is negligible: mean 0.0001% of municipal area in
2025, max 0.069%, and above 0.01% for 8 municipalities.

## 3. Round-trip against the source

The long-form output reproduces the wide source exactly.

| check | result |
|---|---|
| source rows × 41 years | 77,406 × 41 = 3,173,646 |
| rows after summing the state dimension (see §4) | 3,173,605 |
| per-year area totals vs source, exact (`math.fsum`) | max relative difference **0.000e+00**, max absolute **0.000000000 ha** |
| municipality set | identical, 5,572 |
| nulls in `id_municipio`, `id_classe`, `bioma`, `nivel_1..4`, `area` | 0 |
| primary key `(ano, id_municipio, bioma, id_classe)` | unique |

## 4. One duplicate key in the source

`(geocode, biome, class)` is **not** unique in the workbook: **Ibateguara
(2703007, Alagoas)** appears twice for Mata Atlântica / class 15 (Pasture) —
once under `state = Alagoas` (14,543.88 ha in 2025) and once under
`state = Pernambuco` (0.088 ha, constant across all 41 years). A sliver of its
polygon falls on the Pernambuco side of the state layer MapBiomas intersects.

Two consequences, both handled:

- `sigla_uf` is derived from the **first two digits of the municipality code**,
  not from the workbook's `state` column, so the sliver is not filed under PE.
  This is the only disagreement between the two, and the cleaner reports it
  rather than suppressing it.
- The two rows are **summed**, giving 77,405 groups and restoring the stated
  grain. Ibateguara 2025 class 15 is 14,543.967164 ha = 14,543.878807 + 0.088357.

## 5. State table against MapBiomas' own state file

`cobertura_uf_classe` is aggregated up from the municipal table, so the two
published tables cannot disagree. That makes it worth checking against the
figures MapBiomas computes directly over state boundaries, in
`MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx`:

| state-years compared | worst discrepancy | over 1% |
|---|---|---|
| 1,107 | **+0.090%** (Roraima, 2006) | 0 |

## 5b. Transitions against coverage

A transition matrix has a property worth exploiting: summing the 2024→2025
transitions **over the destination class** must recover the 2024 coverage of the
source class, and summing **over the source class** must recover the 2025
coverage of the destination class. If `from` and `to` were swapped, or a period
column mis-parsed, this fails loudly.

| direction | groups compared | worst discrepancy | over 0.5% |
|---|---|---|---|
| Σ over `id_classe_para` vs coverage 2024 of `id_classe_de` | 922 | −0.392% (AP, Amazônia, classe 12) | 0 |
| Σ over `id_classe_de` vs coverage 2025 of `id_classe_para` | 929 | −0.394% (AP, Amazônia, classe 12) | 0 |

The residual is the same municipality-vs-state raster difference measured in §5:
the transition matrix is computed over state boundaries, the coverage table is
aggregated from municipal ones.

## 5c. dbt, against BigQuery

`dbt run` materialises into `basedosdados-dev.br_mapbiomas_estatisticas`. Every
row count BigQuery reports matches the local parquet exactly:

| table | local rows | BigQuery rows |
|---|---|---|
| `cobertura_municipio_classe` | 3,173,605 | 3,173,605 |
| `cobertura_uf_classe` | 39,360 | 39,360 |
| `transicao_uf_de_para_anual` | 566,560 | 566,560 |
| `transicao_uf_de_para_quinquenal` | 113,312 | 113,312 |
| `transicao_uf_de_para_decenal` | 42,492 | 42,492 |
| `classe` | 34 | 34 |

`dbt run`: PASS=6, ERROR=0. `dbt test`: **PASS=65, FAIL=0, ERROR=2**.

The checks that matter all pass:

- `dbt_utils.unique_combination_of_columns` on
  `(ano, id_municipio, bioma, id_classe)` over 3.17M rows — confirming the
  Ibateguara fix in §4 against the materialised table, not just locally — and on
  the equivalent key of every other table.
- `relationships` on `id_municipio` → the municipality directory, with the two
  lagoon codes excluded by `where`.
- `relationships` on `ano` and `ano_inicial` → the time directory, bound as
  `ano.ano`. The time directory's column is a STRUCT; a bare `ano` matches
  nothing silently.
- `relationships` on `id_classe` / `id_classe_de` / `id_classe_para` →
  `classe.chave`, and on `sigla_uf` → the UF directory.

**The two errors are not data failures.** Both are on
`transicao_uf_de_para_quinquenal` (`id_classe_para` and `sigla_uf` relationships)
and both report `Custom quota exceeded ... QueryUsagePerDay` — the project's
daily BigQuery byte ceiling, hit at the end of the run. The identical tests pass
on the two sibling tables, which have the same structure and the same class and
UF domains, and both constraints were checked directly against the source data:

```
transicao_uf_de_para_quinquenal:
   id_classe_de   not in classe.chave: none
   id_classe_para not in classe.chave: none
   sigla_uf not a valid UF: none   (27 distinct)
```

They should still be re-run once the quota resets, to close the loop in dbt
itself rather than beside it.

## 5d. Metadata registered on staging

| order | table | columns | coverage | observation levels | cloud table |
|---|---|---|---|---|---|
| 0 | `cobertura_municipio_classe` | 10 | 1985–2025 | municipality, year, terrain | `basedosdados-dev` |
| 1 | `cobertura_uf_classe` | 9 | 1985–2025 | state, year, terrain | `basedosdados-dev` |
| 2 | `transicao_uf_de_para_anual` | 7 | 1986–2025 | state, year, terrain | `basedosdados-dev` |
| 3 | `transicao_uf_de_para_quinquenal` | 7 | 1990–2025 (5) | state, year, terrain | `basedosdados-dev` |
| 4 | `transicao_uf_de_para_decenal` | 7 | 2000–2020 (10) | state, year, terrain | `basedosdados-dev` |
| 5 | `classe` | 21 | — | terrain | `basedosdados-dev` |
| 6–8 | `transicao_municipio_de_para_*` | 6 | stale | **unlinked** | `basedosdados` |

Every observation level is linked to the column that identifies it. The earlier
registration recorded the land-cover class dimension against the `unknown` and
`other` entities with no column link, which the site renders as "Não informado";
those were replaced with `terrain` linked to the class column, matching what this
dataset's own `classe` table already used.

Rows 6–8 are the three tables with no source. They were left exactly as they
were, sorted last, and still carry the previous collection's coverage and cloud
tables pointing at `basedosdados`.

## 6. Legend

The published legend and the statistics workbook disagree, and the divergence is
recorded rather than smoothed over:

- **Class 13** (`Mosaico Herbáceo-Arbustivo`) appears in the statistics and in
  neither legend artifact. **Class 77** (`Formação Herbáceo Arbustiva`) appears
  in both legend artifacts and in no statistics row.
- **Class 0** (`Não Observado`) appears in the statistics and in no legend.
- The two sources number the hierarchy differently — Savanna Formation is 1.2 in
  the workbook and 1.3 in the legend PDF; Grassland is 2.3 and 2.1. The numeric
  prefix is therefore **not** carried into the published labels; `chave` is the
  stable identifier, and `codigo_hierarquia` records the workbook's numbering.
- The legend sheet numbers class 48 (`Outras Lavouras Perenes`) as `3.2.1.4`,
  which is already Cotton; the workbook's `3.2.2.4` is used.

Spanish labels are a Data Basis translation. MapBiomas Brasil publishes no
Spanish legend, and the Spanish-language initiatives (Chaco, Amazonía) use a
different class list, so their terms could not be borrowed. The build fails if
the workbook ever emits a class label with no Portuguese/Spanish entry
(`check_label_coverage`).

## 7. What could not be built

MapBiomas publishes transitions by **biome and state only**. There is no
municipal transition statistic in Collection 11, nor in Collection 10.1, nor in
any archived version of the statistics page. These three registered tables
therefore have no source and remain empty:

- `transicao_municipio_de_para_anual`
- `transicao_municipio_de_para_quinquenal`
- `transicao_municipio_de_para_decenal`

Building them would require computing transition matrices from the MapBiomas
Earth Engine asset, which is separate work.

MapBiomas 10 m is **Collection 4**; its statistics are not published (the page
says "conteúdo em elaboração"), and its predecessor at 10 m published biome and
region totals only, never municipality. There is no 10 m municipal coverage to
add.

Eleven ad-hoc transition periods in the source (`p1985_2025`, `p2008_2017`,
`p1994_2002`, `p2010_2016`, `p1986_2015`, `p1990_2025`, `p2000_2025`,
`p2002_2010`, `p2008_2025`, `p2010_2025`, `p2012_2025`) belong to none of the
three registered transition tables and are skipped.
