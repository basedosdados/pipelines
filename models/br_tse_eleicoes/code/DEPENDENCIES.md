# `br_tse_eleicoes` — Table Dependency Map

The DBT models under `models/br_tse_eleicoes/*.sql` are thin `safe_cast` passthroughs
over staging tables. All real logic lives in `code/python/`. This document maps,
for every published table, the modules and other tables it depends on.

## Pipeline phases

| Phase | Driver | Role |
|-------|--------|------|
| 1 — Build | `code/python/sub/*.py` (called from `build.py`) | Read raw TSE CSVs → per-year parquets |
| 2 — Normalize & partition | `code/python/normalization_partition.py` | Build `norm_candidatos`; enrich with `titulo_eleitoral`; write Hive-partitioned outputs |
| 3 — Aggregate | `code/python/aggregation.py` | Roll up municipio_zona → municipio; concat historical + modern; party-level rollups |

`normalization_partition.norm_candidatos` is the linchpin: every table that
carries `titulo_eleitoral_candidato` joins against it in Phase 2.

---

## Per-table dependencies

Columns: **Builder** = Phase 1 module:function | **Raw input** = `INPUT_DIR/...` pattern | **Phase 2 merge** = enrichment in `normalization_partition.py` | **Phase 3 agg** = aggregation source | **Upstream tables** = other staging tables it needs | **Utils**.

### Candidatos & Partidos

| Table | Builder | Raw input | Phase 2 merge | Phase 3 agg | Upstream tables | Utils |
|---|---|---|---|---|---|---|
| `candidatos` | `sub/candidates.py::build_candidatos(ano)` (1994–2024) | `consulta_cand/consulta_cand_{ano}/consulta_cand_{ano}_{uf}` + `consulta_cand_complementar_{ano}/...` (2024) | `normalize_candidates()` — dedup by `titulo_eleitoral` via 5 rules; merge CPF↔título mapping → **produces `norm_candidatos`** | — | — | `clean_education`, `clean_election_type`, `clean_marital_status`, `clean_party`, `clean_result`, `clean_string`, `fix_candidate`, `helpers.{clean_nulls, merge_municipio, pad_cpf, pad_titulo, parse_date_br, read_raw_csv}` |
| `partidos` | `sub/parties.py::build_partidos(ano)` (1990–2024) | `consulta_coligacao/CONSULTA_LEGENDA_{ano}/…` (+ legendas/coligacao variants) | dedup `(ano, turno, tipo_eleicao, sigla_uf, id_municipio_tse, cargo, numero)`; drop "partido isolado" when "coligacao" exists | — | — | `clean_election_type`, `clean_party`, `clean_string`, `helpers.merge_municipio` |

### Campaign finance

| Table | Builder | Raw input | Phase 2 merge | Phase 3 agg | Upstream tables | Utils |
|---|---|---|---|---|---|---|
| `bens_candidato` | `sub/campaign_finance.py::build_bens(ano)` (2006–2024) | `bem_candidato/bem_candidato_{ano}/bem_candidato_{ano}_{uf}` | `_partition_bens(norm_cand)` — left-merge on `(ano, tipo_eleicao, sigla_uf, sequencial)` → adds `titulo_eleitoral_candidato` | — | `candidatos` (via `norm_candidatos`) | `clean_election_type`, `clean_string`, `helpers.{parse_date_br, read_raw_csv}` |
| `receitas_candidato` | `sub/campaign_finance.py::build_receitas(ano)` (2002–2024) | `prestacao_contas/prestacao_contas_{ano}/...` (2002–2010); `prestacao_final_{ano}/receitas_candidatos_{ano}_{uf}` (2012–2016); `prestacao_de_contas_eleitorais_candidatos_{ano}/receitas_candidatos_{ano}_{uf}` (2018+) | `_partition_finance("receitas_candidato", …)` — merge on `(ano, tipo_eleicao, sigla_uf, id_municipio_tse, cargo, numero)` → adds `titulo_eleitoral`, `numero_partido`, `sigla_partido` | — | `candidatos` | `clean_election_type`, `clean_string`, `helpers.{merge_municipio, parse_date_br, read_raw_csv}` |
| `despesas_candidato` | `sub/campaign_finance.py::build_despesas(ano)` (2002–2024) | same family as receitas (`prestacao_*/despesas_candidatos_*`) | `_partition_finance("despesas_candidato", …)` — same merge keys as receitas | — | `candidatos` | same as receitas |
| `receitas_comite` | `aggregation.py::build_receitas_comite()` | Phase-2 `receitas_candidato` partitions | — | groupby party committee + date; sum `valor_receita` | `receitas_candidato` | — |
| `receitas_orgao_partidario` | `aggregation.py::build_receitas_orgao_partidario()` | Phase-2 `receitas_candidato` partitions | — | groupby `(numero_partido, sigla_partido)`; sum `valor_receita` | `receitas_candidato` | — |

### Resultados — modern (1994+)

| Table | Builder | Raw input | Phase 2 merge | Phase 3 agg | Upstream tables | Utils |
|---|---|---|---|---|---|---|
| `resultados_candidato_secao` | `sub/results_section.py::build_resultados_secao(ano)[0]` (1994–2024) | `votacao_secao/votacao_secao_{ano}_{uf}/votacao_secao_{ano}_{uf}` | merge with `norm_candidatos` → `titulo_eleitoral_candidato` | — | `candidatos` | `clean_election_type`, `clean_string`, `helpers.{clean_nulls, merge_municipio, parse_date_br, read_raw_csv}` |
| `resultados_partido_secao` | `sub/results_section.py::build_resultados_secao(ano)[1]` | same as candidato_secao | partitioned, **no título merge** (party-level) | — | — | same |
| `resultados_candidato_municipio_zona` | `sub/results_mun_zone.py::_build_candidato(ano)` (1994–2024) | `votacao_candidato_munzona/votacao_candidato_munzona_{ano}/…_{uf}` | merge with `norm_candidatos` → `titulo_eleitoral_candidato`, `numero_partido`, `sigla_partido` | — | `candidatos` | `clean_election_type`, `clean_party`, `clean_result`, `clean_string`, `helpers.*` |
| `resultados_partido_municipio_zona` | `sub/results_mun_zone.py::_build_partido(ano)` | `votacao_partido_munzona/.../…_{uf}` | partitioned, **no título merge** | — | — | `clean_election_type`, `clean_party`, `clean_string`, `helpers.*` |
| `resultados_candidato_municipio` | `aggregation.py::build_resultados_candidato_municipio()` | Phase-2 `resultados_candidato_municipio_zona` | — | groupby municipio (drop zona); sum `votos` | `resultados_candidato_municipio_zona` | — |
| `resultados_partido_municipio` | `aggregation.py::build_resultados_partido_municipio()` | Phase-2 `resultados_partido_municipio_zona` | — | groupby municipio; sum `votos_*` | `resultados_partido_municipio_zona` | — |
| `resultados_candidato` (all-years) | `aggregation.py::build_resultados_candidato()` | modern: Phase-2 `resultados_candidato_municipio_zona`; historical: `resultados_candidato_uf_{ano}.parquet` | — | concat historical (1945–1990) + modern; merge `norm_candidatos`; clear `sigla_uf` for presidentes, clear `id_municipio` for federais | `resultados_candidato_municipio_zona`, `candidatos`, `sub/results_state.py` outputs | — |

### Resultados — histórico (1945–1990)

Built by `sub/results_state.py` and consumed only by the all-years `resultados_candidato` rollup. Not exposed as standalone DBT models in the current schema (`resultados_candidato_uf` / `resultados_partido_uf` are intermediate parquets).

| Intermediate | Builder | Raw input | Utils |
|---|---|---|---|
| `resultados_candidato_uf` | `sub/results_state.py::build_candidato(ano)` (1945, 1947, 1950–1986, 1989, 1990) | `votacao_candidato_uf/VOTACAO_CANDIDATO_{ano}_{uf}/…` | `clean_election_type`, `clean_party`, `clean_result`, `clean_string`, `fix_candidate` |
| `resultados_partido_uf` | `sub/results_state.py::build_partido(ano)` (same set, except 1989) | `votacao_partido_uf/VOTACAO_PARTIDO_{ano}_{uf}/…` | same |

### Detalhes de votação

| Table | Builder | Raw input | Phase 2 merge | Phase 3 agg | Upstream tables | Utils |
|---|---|---|---|---|---|---|
| `detalhes_votacao_secao` | `sub/voting_details_section.py::build_detalhes_secao(ano)` (1994–2024) | `detalhe_votacao_secao/detalhe_votacao_secao_{ano}/…_{uf}` | partitioned, no merge | — | — | `clean_election_type`, `clean_string`, `helpers.*` |
| `detalhes_votacao_municipio_zona` | `sub/voting_details_mun_zone.py::build_detalhes_mun_zona(ano)` (1994–2024) | `detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}/…_{uf}` | partitioned, no merge | — | — | same |
| `detalhes_votacao_municipio` | `aggregation.py::build_detalhes_votacao_municipio()` | Phase-2 `detalhes_votacao_municipio_zona` | — | groupby municipio; sum `aptos`, `comparecimento`, `votos_*`; **recompute** `proporcao_*` | `detalhes_votacao_municipio_zona` | — |
| `detalhes_votacao_uf` (historical) | `sub/voting_details_state.py` (1945–1990) | `detalhe_votacao_uf/DETALHE_VOTACAO_UF_{ano}/…` | — | — | — | `clean_election_type`, `clean_string` |

### Perfil do eleitorado

| Table | Builder | Raw input | Phase 2 merge | Phase 3 agg | Upstream tables | Utils |
|---|---|---|---|---|---|---|
| `perfil_eleitorado_secao` | `sub/voter_profile_section.py::build_perfil_secao(ano)` (2008–2024) | `perfil_eleitorado_secao/perfil_eleitor_secao_{ano}_{uf}.txt` (named-column CSV) | groupby demographics (`genero, estado_civil, grupo_idade, instrucao`); sum eleitores* | — | — | `helpers.merge_municipio` |
| `perfil_eleitorado_municipio_zona` | `sub/voter_profile_mun_zone.py::build_perfil_mun_zona(ano)` (1994–2024) | `perfil_eleitorado/perfil_eleitorado_{ano}/perfil_eleitorado_{ano}` (single national file) | partitioned, no merge | — | — | `helpers.merge_municipio` |
| `perfil_eleitorado_local_votacao` | `sub/voter_profile_polling_place.py::build_perfil_local_votacao()` (2010–2024) | `perfil_eleitorado_local_votacao/eleitorado_local_votacao_{ano}/eleitorado_local_votacao_{ano}` | **all years concatenated into ONE parquet** (unlike all other tables) | — | — | `helpers.merge_municipio` |

### Outras

| Table | Builder | Raw input | Phase 2 / 3 | Upstream tables | Utils |
|---|---|---|---|---|---|
| `vagas` | `sub/vacancies.py::build_vagas(ano)` (1994–2024, even years) | `consulta_vagas/consulta_vagas_{ano}/consulta_vagas_{ano}_{uf}` | — | — | `clean_election_type`, `clean_string`, `helpers.*` |
| `dicionario` | static — not built by the Python pipeline | — | — | — | — |

---

## Cross-table dependency graph

```
                    ┌────────────────────────────────────────┐
                    │            RAW TSE CSVs                │
                    └─┬───┬───┬───┬───┬───┬───┬───┬───┬───┬──┘
                      │   │   │   │   │   │   │   │   │   │
        ┌─────────────┘   │   │   │   │   │   │   │   │   └──────────────────┐
        ▼                 ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼                      ▼
   candidates.py     parties.py  campaign_   results_   voting_details_*  voter_profile_*
        │                 │     finance.py   {section,    {section,         {section,
        │                 │       │  │  │     mun_zone,    mun_zone,         mun_zone,
        │                 │       │  │  │     state}.py    state}.py         polling_place}.py
        │                 │     bens receitas │            │                  │
        │                 │          despesas │            │                  │
        │                 │       │  │  │     │            │                  │
   ┌────▼─────┐           │       │  │  │  ┌──▼─────────┐  │                  │
   │candidatos│           │       │  │  │  │ resultados │  │                  │
   │ (raw)    │           │       │  │  │  │ secao/     │  │                  │
   └────┬─────┘           │       │  │  │  │ mun_zone/  │  │                  │
        │                 │       │  │  │  │ uf (hist)  │  │                  │
        ▼                 │       │  │  │  └──┬─────────┘  │                  │
   normalization_partition.py: norm_candidatos │            │                  │
        │   │   │   │                          │            │                  │
        │   │   │   └────────────merge titulo──┤            │                  │
        │   │   └────────────merge titulo──────┼────────────┤                  │
        │   └────────merge titulo──────────────┘            │                  │
        │                                                   │                  │
        ▼ (writes partitioned parquets for ALL phase-1 tables; some with titulo enrichment)
   ┌────────────────────────────────────────────────────────────────────────────┐
   │  Partitioned: candidatos/, partidos/, bens_candidato/, receitas_candidato/, │
   │  despesas_candidato/, resultados_*_secao/, resultados_*_municipio_zona/,    │
   │  detalhes_votacao_{secao,municipio_zona}/, perfil_eleitorado_*/, vagas/     │
   └────┬───────────────────┬───────────────────┬───────────────────┬───────────┘
        │                   │                   │                   │
        ▼                   ▼                   ▼                   ▼
   aggregation.py:                                          (campaign finance
   resultados_candidato_municipio                            rollups)
   resultados_partido_municipio                              receitas_comite
   resultados_candidato (all-years + hist)                   receitas_orgao_partidario
   detalhes_votacao_municipio
```

### Critical fan-out from `candidatos` / `norm_candidatos`

Every table that carries `titulo_eleitoral_candidato` was enriched at Phase 2
via a left-merge against `norm_candidatos`:

```
candidatos ──► norm_candidatos ──►  bens_candidato
                              ──►  receitas_candidato ──► receitas_comite
                              │                       ──► receitas_orgao_partidario
                              ──►  despesas_candidato
                              ──►  resultados_candidato_secao
                              ──►  resultados_candidato_municipio_zona
                                            │
                                            ├──► resultados_candidato_municipio (agg)
                                            └──► resultados_candidato (all-years agg)
```

### Aggregation chain (Phase 3)

```
resultados_candidato_municipio_zona ─► resultados_candidato_municipio
                                    ─► resultados_candidato (concat w/ historical UF)

resultados_partido_municipio_zona   ─► resultados_partido_municipio

detalhes_votacao_municipio_zona     ─► detalhes_votacao_municipio  (recomputes proporcao_*)

receitas_candidato                  ─► receitas_comite
                                    ─► receitas_orgao_partidario
```

---

## Utilities cheat-sheet (`code/python/utils/`)

| Module | What it does | Used by |
|---|---|---|
| `clean_string.py` | `clean_string_series` — strip, uppercase, deaccent | nearly all sub/*.py |
| `clean_election_type.py` | `clean_election_type_series(s, ano)` — normalize `tipo_eleicao` per year | almost all |
| `clean_party.py` | normalize partido names / numbers | candidates, parties, results_* |
| `clean_education.py` | canonicalize escolaridade codes | candidates |
| `clean_marital_status.py` | canonicalize estado civil | candidates |
| `clean_result.py` | canonicalize `resultado` (eleito, suplente, etc.) | candidates, results_mun_zone, results_state |
| `fix_candidate.py` | hand-coded fixes for historical candidate rows | results_state |
| `helpers.read_raw_csv` | reads `;`-delimited Latin-1 TSE files | all sub/*.py |
| `helpers.parse_date_br` | DD/MM/YYYY → YYYY-MM-DD | most builders |
| `helpers.merge_municipio` | join TSE municipality codes to BD `id_municipio` | every table with municipal grain |
| `helpers.clean_nulls` | maps `#NULO#`, `#NULO`, `-1` → `""` | most builders |
| `helpers.pad_cpf` / `pad_titulo` | left-zero-pad CPF / título eleitoral | candidates, campaign_finance |
| `helpers.save_partitioned` | writes Hive-partitioned parquets (`ano=YYYY/sigla_uf=XX/...`) | `normalization_partition.py`, `aggregation.py` |

---

## Notes / gotchas

- **`titulo_eleitoral` is the canonical candidate key.** CPF is not unique across elections; título is. The merge into `norm_candidatos` is what makes downstream candidate-level tables joinable.
- **`sigla_uf = 'BR'`** (federal/presidential candidates in raw TSE) is normalized to empty string everywhere (BD convention; see `feedback_sigla_uf_convention.md`).
- **`perfil_eleitorado_local_votacao`** is the only table that concatenates all years into a single parquet rather than per-year files.
- **Historical resultados (1945–1990)** flow through `sub/results_state.py` and `sub/voting_details_state.py` only — they have different schemas and are only surfaced via the all-years `resultados_candidato` aggregation.
- **Year cadence:** results, voting details, campaign finance, vagas → even years only (1994/2002/2006 starts depending on the table). Candidatos and partidos cover odd years too where data exists.
- **`dicionario`** is curated by hand (not built by Python).
- **Architecture table wins** over raw/DBT names where they disagree (per repo CLAUDE.md).
