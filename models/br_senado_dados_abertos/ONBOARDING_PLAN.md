# br_senado_dados_abertos — Onboarding Plan

Parallel to `br_camara_dados_abertos`. Org `senado` (area `br`). Consolidated single dataset.
GCP dataset id / BD slug: **`br_senado_dados_abertos`** (mirrors the Câmara's `br_camara_dados_abertos`).

## Source

- **Legislative API** `https://legis.senado.leg.br/dadosabertos` — public, **no API key**. JSON via
  `Accept: application/json` (XML default). OpenAPI at `/dadosabertos/v3/api-docs`.
  - Two response styles: newer endpoints return flat camelCase arrays (`/votacao`, `/composicao/lideranca`);
    older ones return capitalized nested envelopes (`ListaPartidos.Partidos.Partido[]`) → flatten.
  - **Intermittent empty (HTTP-empty) responses under rapid calls → crawler needs retry + backoff.**
- **Transparency portal bulk CSV** `https://www12.senado.leg.br/transparencia/dados-abertos-transparencia`
  — administrative (T3): CEAPS expenses, contracts, personnel. Separate source.

## Câmara conventions to mirror

IDs → STRING; vote tallies → INT64 (no unit, house convention); dates → DATE, timestamps split DATE+TIME;
birth municipality joined to `basedosdados.br_bd_diretorios_brasil.municipio` on (nome, sigla_uf);
bills partitioned by `ano` INT64; column names Portuguese snake_case.

---

## Tier 1 — core (this pass), ~10 tables

| # | table | grain | source endpoint(s) | partition | temporal |
|---|-------|-------|--------------------|-----------|----------|
| 1 | `senador` | one row per senator (identity, deduped) | iterate `/senador/lista/legislatura/{ini}/{fim}` (all historical legislatures), enrich `/senador/{codigo}` | none (dim) | full roster history |
| 2 | `votacao` | one row per nominal vote | `/votacao?dataInicio&dataFim` (drop `votos[]`), iterate years | `ano` (year of data_sessao) | **1991→** |
| 3 | `votacao_parlamentar` | one row per (vote × senator) | explode `/votacao`.`votos[]` | `ano` | **1991→** |
| 4 | `votacao_orientacao_bancada` | one row per (vote × bancada/liderança) | explode `/plenario/votacao/orientacaoBancada/{ini}/{fim}`.`votacoes[].orientacoesLideranca[]` | `ano` | as available |
| 5 | `processo` | one row per legislative process (bill) | `/processo?ano=` iterate years (+`sigla`) | `ano` (process year) | broad |
| 6 | `partido` | one row per party (incl. extinct) | `/composicao/lista/partidos` | none (dim) | 1935→ (has data_criacao/extincao) |
| 7 | `bloco` | one row per parliamentary bloc | `/composicao/lista/blocos` | none (dim) | current + creation dates |
| 8 | `lideranca` | one row per leadership designation | `/composicao/lideranca` (flat) | none | historical designations |
| 9 | `comissao` | one row per collegiate body | `/comissao/lista/tiposColegiado` → `/comissao/lista/{tipo}` | none (dim) | active (+ as available) |
| 10 | `mesa` | one row per Mesa Diretora seat | `/composicao/mesaSF` (+ `/mesaCN`) | none | **current snapshot only** ⚠ |

### Column sketches (final columns land in the architecture sheets)

**senador**: `id_senador` (CodigoParlamentar, STRING), `nome`, `nome_completo`, `sexo`,
`forma_tratamento`, `id_municipio_nascimento`+`sigla_uf_nascimento` (if available via detail),
`email`, `url_foto`, `url_pagina`.

**votacao**: `ano` (part), `id_votacao` (codigoSessaoVotacao), `data_sessao`, `casa`, `id_sessao`,
`id_sessao_legislativa`, `id_processo`, `codigo_materia`, `identificacao_materia`, `sigla_materia`,
`numero_materia`, `ano_materia`, `sigla_tipo_sessao`, `numero_sessao`, `descricao_votacao`, `ementa`,
`resultado_votacao`, `votacao_secreta` (STRING bool), `voto_sim`/`voto_nao`/`voto_abstencao` (INT64),
`sigla_colegiado`, `nome_colegiado`.

**votacao_parlamentar**: `ano` (part), `id_votacao`, `data_sessao`, `id_senador`, `nome_parlamentar`,
`sexo`, `sigla_partido`, `sigla_uf`, `voto` (Sim/Não/Abstenção/…), `descricao_voto`.

**votacao_orientacao_bancada**: `ano` (part), `sequencial_votacao`, `id_votacao_sve`, `sigla_materia`,
`numero_materia`, `ano_materia`, `data_votacao`, `bancada`, `orientacao` (SIM/NÃO/LIVRE/…).

**processo**: `ano` (part), `id_processo`, `codigo_materia`, `identificacao`, `sigla`, `numero`,
`autoria`, `ementa`, `objetivo`, `tipo_documento`, `tipo_conteudo`, `situacao_atual`,
`data_situacao_atual`, `data_apresentacao`, `tramitando`, `ente_identificador`, `casa_identificadora`,
`url_documento`, `data_ultima_atualizacao`.

**partido**: `id_partido`, `sigla_partido`, `nome_partido`, `data_criacao`, `data_extincao`.
**bloco**: `id_bloco`, `nome_bloco`, `nome_apelido`, `data_criacao`, `data_extincao`.
**lideranca**: `id_lideranca`, `casa`, `id_senador`, `nome_parlamentar`, `id_partido_filiacao`,
`sigla_partido_filiacao`, `sigla_tipo_lideranca`, `descricao_tipo_lideranca`,
`sigla_tipo_unidade_lideranca`, `descricao_tipo_unidade_lideranca`, `data_designacao`.
**comissao**: `id_comissao`, `sigla_comissao`, `nome_comissao`, `tipo_colegiado`, `casa`,
`data_inicio`, `data_fim` (field list finalized against live `ListaBasicaComissoes`).
**mesa**: `id_colegiado`, `sigla_colegiado`, `nome_colegiado`, `cargo`, `id_senador`,
`nome_parlamentar`, `bancada`, `ordem`.

### Directory FKs & dbt
- `sigla_uf` → `br_bd_diretorios_brasil.uf:sigla_uf`; `ano` → `br_bd_diretorios_data_tempo.ano:ano`.
- `id_senador` references the local `senador` table; `id_partido`/`sigla_partido` reference local `partido`
  (dbt `relationships` tests, like Câmara's local refs).
- Each model: `unique_combination_of_columns` on (partition + key), `not_null_proportion_multiple_columns` 0.05.

### Caveats to confirm
- ⚠ `mesa` and committee **composition** are current-snapshot only from these endpoints; historical Mesa/committee membership is limited. Kept as snapshot in T1; historical membership deferred/omitted.
- Vote tallies as INT64 with no measurement_unit (mirrors Câmara house convention).
- Senator identity has no BD directory; the `senador` table is the dataset's own dimension.

---

## Tier 2 — legislative (later): discurso/pronunciamento, plenary `sessao`, `votacao_comissao`,
senador sub-tables (mandato, filiacao, cargo, comissao membership, profissao, historico_academico),
processo authorship/relatoria/emenda, bloco/comissao membership bridges.

## Tier 3 — administrative (later, transparency CSV): `despesa_ceaps`, `contratacoes`, `gestao_pessoas`.
Then deprecate/delete the 5 old empty `senado` shells (senadores_legislativo, senadores_administrativo,
projetos_e_materias_do_senado_federal_legislativo, contratacoes_do_senado_federal,
gestao_de_pessoas_do_senado_federal_administrativo).

## Pipelines & BD Pro
Recurring Prefect flows (parallel to Câmara's per-table daily flows). Per-table tier: high-frequency
tables (votacao*, processo) → `PartBdpro(free_lag=6 months)` rolling window (create pro Coverage before
arming); low-frequency dims (partido, comissao, senador) → `AllFree`.
