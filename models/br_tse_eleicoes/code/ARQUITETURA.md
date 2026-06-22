# Arquitetura — `br_tse_eleicoes` (pós-refatoração)

Visão geral dos arquivos e do fluxo de dependências depois da migração
(parsing header-based + rollups dbt + orquestração Prefect 3). Decisões em
[`decisoes_migracao.md`](decisoes_migracao.md); grafo de tabelas-fonte em
[`DEPENDENCIES.md`](DEPENDENCIES.md).

## Duas camadas

A refatoração separa **o que limpa os dados** de **o que orquestra na cloud**:

```
┌──────────────────────────────────────────────────────────────────────┐
│ CAMADA 1 — limpeza + validação   (models/br_tse_eleicoes/code/python)  │
│   builders Python (todos os anos) + harness de diagnóstico             │
│   → escreve parquets particionados em $TSE_DATA_DIR/output_python       │
└───────────────────────────────┬──────────────────────────────────────┘
                                 │ invocada por subprocess (run_build_step)
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│ CAMADA 2 — orquestração Prefect 3   (pipelines/br_tse_eleicoes)        │
│   1 flow/deployment por tabela + orquestrador __refresh (wait_for)     │
│   → upload_to_gcs (staging) + run_dbt                                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                 │ run_dbt materializa
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│ MODELOS dbt   (models/br_tse_eleicoes/*.sql)                           │
│   producers: safe_cast da staging · rollups: ref()+GROUP BY in-warehouse│
└──────────────────────────────────────────────────────────────────────┘
```

---

## Camada 1 — limpeza + validação (`code/python`)

### Pipeline de dados (3 fases — `build.py`)

| Arquivo | Papel |
|---|---|
| `build.py` | Entry point. `STEPS` (builders fase-1) → `normalize` (fase-2) → `aggregate` (fase-3). |
| `config.py` | Paths (`TSE_DATA_DIR`, `OUTPUT_PYTHON`), listas de UF por ano, faixas de ano. |
| `utils/helpers.py` | `read_raw_csv(...)` — **agora** aceita `family`/`ano` e renomeia `vN`→nome oficial via `resolve_columns` (header-based). `merge_municipio`, `clean_nulls`, etc. |
| `utils/layout.py` **(novo)** | `resolve_columns(df, family, ano)` e `layout_columns(family, ano)` — leem os artifacts de layout do harness. **É a ponte** entre runtime e validação. |
| `utils/clean_*.py`, `fix_candidate.py` | Normalização de domínios (partido, escolaridade, resultado, …). |
| `sub/*.py` | **Builders fase-1** (um módulo por família raw). Convertidos: mapeiam colunas por **nome oficial TSE** (dicts `cols`/`col_map`), não mais por posição. |
| `normalization_partition.py` | **Fase 2**: constrói `norm_candidatos` (linchpin do título eleitoral) e escreve parquets Hive-particionados de todas as tabelas. |
| `aggregation.py` | **Fase 3**: rollups Python. 3 deles (`*_municipio`) saem no Track C → viram dbt; permanecem `resultados_candidato`, `receitas_comite`, `receitas_orgao_partidario`. |

### Harness de diagnóstico (`code/python/diagnostics/`) — o gate de validação

| Arquivo | Papel |
|---|---|
| `spec.py` | Fonte de verdade: `FAMILIES` (famílias raw + URLs leiame), `FUNC_SPECS` (builder→tabela→anos→família). |
| `tier1_audit.py` | Extrai por AST os sites de mapeamento de cada builder; classifica **positional** (`vN`) vs **named** (var `cols`/`keep_cols`/`col_map` com chaves não-`vN`). |
| `tier2_leiame.py` | Adquire o layout oficial por `(família, ano)` (header real → HTTP Range → leiame PDF) → cacheia em `artifacts/layouts/{family}_{ano}.json`. |
| `tier3_check.py` | Cruza tier1 × tier2: **named** → toda chave selecionada deve existir no layout; **positional** → nome oficial na posição N deve ser afinidade-compatível. Gera `artifacts/findings.json`. |
| `affinity.py` | Compatibilidade nome-BD × nome-TSE (para sites posicionais). |
| `report.py` | Regenera `DIAGNOSIS.md` e a matriz de status. |
| `artifacts/layouts/*.json` | 243 layouts oficiais cacheados — **consumidos por `layout.py` (runtime) e por `tier3` (validação)**. |

### Por que a correção é mecanicamente verificável

```
        artifacts/layouts/{family}_{ano}.json   (nomes oficiais TSE por posição)
              │                              │
   (runtime) │                              │ (validação estática)
              ▼                              ▼
   utils/layout.resolve_columns        diagnostics/tier3._check_named
   renomeia vN → nome oficial          confirma que toda chave existe
              │                              │
              ▼                              ▼
   builder seleciona por nome  ─────►  tier1 reclassifica positional→named
   {"NR_PARTIDO": "numero_partido"}    → FAIL vira OK quando os nomes batem
```
Como ambos leem o **mesmo** JSON, o rename em runtime nunca diverge do que o
harness valida (Decisão D2/D3).

### Documentos
`DEPENDENCIES.md` (grafo de tabelas), `DIAGNOSIS.md` (narrativa+evidência),
`diagnosis/*.md` (por tabela), `PROPOSAL_dbt_rollups.md` (plano), `decisoes_migracao.md`.

---

## Camada 2 — orquestração Prefect 3 (`pipelines/br_tse_eleicoes/`)

| Arquivo | Papel |
|---|---|
| `constants.py` | `TABLE_FLOWS`: o grafo topológico (tabela → perfil, build_step, wave, `wait_for`). `constants.CODE_DIR`/`BUCKET`. |
| `utils.py` | `flow_name(table)` e `deployment_name(table)` (para `run_deployment`). |
| `tasks.py` | `run_build_step(step)` — roda `build.py <step>` via subprocess na Camada 1; `output_path_for(table)`. |
| `flows.py` | Fábrica `_tse_table_flow` → 1 `@flow`/deployment por tabela; orquestrador `br_tse_eleicoes__refresh` (`run_deployment` + `.submit(wait_for=...)`). |
| `schedules.py` | `deploy_schedules` (só o orquestrador agenda; tabelas são disparadas por ele). |

`deploy_flows.py --all` varre `pipelines/**` e registra os **22 deployments**
(21 tabelas + `__refresh`). Três perfis de flow:
`producer` (build→normalize→upload→dbt) · `dbt_rollup` (só `run_dbt`) ·
`python_agg` (aggregate→upload→dbt).

---

## Grafo de dependências

### (a) Fluxo de dados — fases do `build.py`

```
RAW TSE CSVs (republicados c/ header)
   │  read_raw_csv(family, ano) → resolve_columns (vN→nome oficial)
   ▼
FASE 1  sub/*.py  (por família, todos os anos)
   candidates · parties · vacancies · campaign_finance(bens/receitas/despesas)
   results_{section,mun_zone,state} · voting_details_{section,mun_zone,state}
   voter_profile_{section,mun_zone,polling_place}
   │
   ▼
FASE 2  normalization_partition.py
   candidatos ──► norm_candidatos  (linchpin: título eleitoral)
   merge título em: bens, receitas, despesas, resultados_candidato_secao,
                    resultados_candidato_municipio_zona
   → escreve parquets particionados (ano/sigla_uf) de TODAS as tabelas
   │
   ▼
FASE 3  agregação
   [dbt ref()]  resultados_candidato_municipio   ◄─ resultados_candidato_municipio_zona
                resultados_partido_municipio      ◄─ resultados_partido_municipio_zona
                detalhes_votacao_municipio        ◄─ detalhes_votacao_municipio_zona
   [Python]     resultados_candidato (1945–2024)  ◄─ rcmz + rc_uf(hist) + norm_candidatos
                receitas_comite                   ◄─ receitas_candidato
                receitas_orgao_partidario         ◄─ receitas_candidato
```

### (b) Grafo de orquestração Prefect (`__refresh`, `wait_for`)

```
WAVE 0 (linchpin)
   candidatos                         ── produz norm_candidatos
        │ wait_for
        ▼
WAVE 1
   ├─ dependem de candidatos (merge título):
   │     bens_candidato · receitas_candidato · despesas_candidato
   │     resultados_candidato_secao · resultados_candidato_municipio_zona
   └─ independentes (sem wait):
         partidos · vagas · resultados_partido_secao
         resultados_partido_municipio_zona · detalhes_votacao_{secao,municipio_zona}
         perfil_eleitorado_{secao,municipio_zona,local_votacao}
        │ wait_for
        ▼
WAVE 2 (fase 3)
   resultados_candidato_municipio   wait_for[resultados_candidato_municipio_zona]   (dbt_rollup)
   resultados_partido_municipio     wait_for[resultados_partido_municipio_zona]     (dbt_rollup)
   detalhes_votacao_municipio       wait_for[detalhes_votacao_municipio_zona]       (dbt_rollup)
   resultados_candidato             wait_for[resultados_candidato_municipio_zona, candidatos] (python_agg)
   receitas_comite                  wait_for[receitas_candidato]                    (python_agg)
   receitas_orgao_partidario        wait_for[receitas_candidato]                    (python_agg)
```

> Caveat (D14): `normalize` é uma barreira global da Fase 2 — hoje roda dentro
> de cada flow `producer` (deployment auto-contido), redundante no `__refresh`
> completo. TODO: deployment dedicado de build+normalize do qual os producers
> dependam.

### (c) Mapa de arquivos (alto nível)

```
models/br_tse_eleicoes/
├── *.sql                         modelos dbt (rollups agora ref()+GROUP BY)
├── schema.yml                    testes/descrições
└── code/
    ├── ARQUITETURA.md  decisoes_migracao.md  DEPENDENCIES.md  DIAGNOSIS.md  PROPOSAL_dbt_rollups.md
    ├── diagnosis/*.md            diagnóstico por tabela
    └── python/
        ├── build.py  config.py
        ├── utils/{layout.py⋆, helpers.py, clean_*.py, fix_candidate.py}
        ├── sub/*.py              builders fase-1 (convertidos)
        ├── normalization_partition.py   aggregation.py
        └── diagnostics/{spec,tier1_audit,tier2_leiame,tier3_check,affinity,report}.py
            └── artifacts/{layouts/*.json, findings.json}

pipelines/br_tse_eleicoes/        ⋆ NOVO — orquestração Prefect 3
├── constants.py  utils.py  tasks.py  flows.py  schedules.py
(⋆ = criado/alterado na refatoração)
```
