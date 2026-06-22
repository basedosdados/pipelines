# Proposal — Refatoração integrada `br_tse_eleicoes`: parsing header-based + rollups em dbt, resolvidos na cloud (Prefect)

> **Status:** Plano consolidado · **Escopo:** fix de parsing (todos os builders sinalizados) + 3 rollups → dbt puro + orquestração Prefect na ordem de `DEPENDENCIES.md`

## 1. Contexto e convergência

Dois trabalhos abertos convergem para uma única refatoração:

1. **Rollups → dbt** — mover 3 rollups (`resultados_candidato_municipio`, `resultados_partido_municipio`, `detalhes_votacao_municipio`) do Pandas (`aggregation.py`) para dbt puro (`GROUP BY` via `ref()` sobre as tabelas `*_municipio_zona` publicadas), eliminando 3 árvores de CSV Hive + 3 staging tables.

2. **Correção de dados (`code/diagnosis/*.md`)** — **Causa raiz:** os builders Python mapeiam colunas raw por **índice posicional** (`vN`). Quando o TSE republicou arquivos com colunas inseridas (bloco federação `NR_FEDERACAO`/`SG_FEDERACAO`, demografia, etc.), os mapas `vN→coluna` ficaram dessincronizados. **Fix:** trocar parsing posicional por **lookup pelo nome oficial da coluna TSE** (viável pois arquivos republicados carregam header, confirmado até 1994).

**O ponto de convergência:** as corrupções dos 3 rollups (`resultados_candidato_municipio` votos=0 em 1994; `detalhes_votacao_municipio` colapso de cobertura em 1996; `resultados_candidato` 1994/1996) são **100% herdadas** das tabelas `*_municipio_zona`. Logo, ao (a) corrigir os builders zona com parsing header-based e (b) tornar os rollups dbt puro via `ref()` sobre as tabelas publicadas, **os rollups se autocorrigem** no próximo run — sem fix separado. A ordem `zona→municipio` passa a ser garantida pelo `ref()` do dbt.

**Objetivo:** todos os problemas diagnosticados resolvidos **na run do Prefect, com o código já refatorado rodando na cloud**, respeitando o grafo de dependências de `DEPENDENCIES.md`. Escopo do fix de parsing: **todos os builders sinalizados** (resolve a corrupção latente que o rebuild reintroduziria). Validação: **gate de paridade old-vs-new + smoke-test dos smoking-guns** antes de deletar Python e promover prod.

### ⚠️ Correção a `DEPENDENCIES.md`

`receitas_comite` e `receitas_orgao_partidario` são rollups Python de `receitas_candidato` (groupby + sum), **não** passthroughs — permanecem em `aggregation.py`. São **3** rollups SQL-portáveis, não 5.

---

## 2. Insight reutilizável (não reinventar)

O harness de diagnóstico **já tem** a abstração de layout header-based que os builders precisam:
- `code/python/diagnostics/.../tier2_leiame.py` → `load_layout(family, ano) -> Layout`, com `Layout.columns` = nomes oficiais TSE ordenados (posição 1 = `vN` com N=1). Cadeia de fallback: override → header do arquivo local → header remoto (HTTP Range) → leiame PDF.
- Layouts cacheados em `code/python/diagnostics/artifacts/layouts/{family}_{ano}.json` (ex.: `consulta_cand_2014.json`).
- `affinity.py::compatible()` e `spec.py` (`FAMILIES`, `FUNC_SPECS`) — metadados por família/builder.

Os builders hoje **não consomem** nada disso: `helpers.read_raw_csv()` (`code/python/utils/helpers.py:16`) lê com `header=None` e nomeia `v1..vN`.

---

## 3. Track A — Fix de parsing header-based (todos os builders sinalizados)

**Mecânica comum (reusar `load_layout`):**
- Extrair o loader de layout para um módulo compartilhado `code/python/utils/layout.py` (reusa `diagnostics/.../tier2_leiame.py::load_layout` + artifacts) expondo `resolve_columns(df, family, ano)` que renomeia as colunas `vN` → nome oficial TSE, usando o **header real** quando presente (1994+) e o **JSON de layout cacheado** como override quando ausente (headerless histórico 1945–1990).
- Em cada builder, trocar os dicts year-conditional `{"v26": "situacao", ...}` por seleção **pelo nome oficial** `{"NR_PARTIDO": "numero_partido", ...}`. O nome certo por `(family, ano)` já está nos artifacts de layout/affinity.

**Builders a corrigir, por severidade (ordem topológica de `DEPENDENCIES.md`):**

| Prioridade | Builder | Anos afetados | Sintoma |
|---|---|---|---|
| **CRÍTICO (linchpin)** | `sub/candidates.py::_parse_schema` (L27–124) | 1994–1996, 2014, 2018–2022 | OUT_OF_RANGE + shift; 1996 bloco demográfico 100% NULL; propaga via `norm_candidatos` |
| **CRÍTICO** | `sub/results_mun_zone.py::_build_candidato` (L57+) | 1994–2022 | `votos` lê `SG_FEDERACAO`/`NR_FEDERACAO` → 842k linhas votos=0 em 1994 |
| **CRÍTICO** | `sub/voting_details_mun_zone.py::build_detalhes_mun_zona` (L46+) | 1996, 2000–2016 | colapso 1996 (548 linhas, `aptos_totalizadas=-3`) |
| ALTO | `sub/results_mun_zone.py::_build_partido` (L229+) | 1996, 2016 | `votos_nominais` lê `SQ_COLIGACAO` |
| ALTO | `sub/voter_profile_mun_zone.py::build_perfil_mun_zona` (L20+) | 2008–2016, 2020–2024 | mistura códigos numéricos × labels texto |
| ALTO | `sub/parties.py::build_partidos` (L50+) | 1994–2020 | `cargo` lê `SG_UF`, etc. |
| ALTO | `sub/vacancies.py::build_vagas` (L45+) | 1994–2012 | `vagas` lê `SG_UF`→NULL; `cargo` lê `DT_POSSE` |
| ALTO | `sub/campaign_finance.py` (`_build_despesas` 2014, L1635) | 2014 | `valor_despesa` lê texto |
| WARN/manual | `sub/voter_profile_section.py` (L70, key `cd_mun_sit_biometrica`) | 2008–2022 | MISSING_KEY — verificar nome correto no leiame |
| WARN/manual | `sub/voting_details_state.py` (L81) | 1945–1990 | headerless; revisar layout PDF (positional inevitável) |
| WARN/manual | `sub/campaign_finance.py` (`_build_receitas` 2014 sup., L680) | 2014 sup. | variante não verificada — validar manualmente |
| WARN | `sub/results_section.py` (L71,L100) | 1994–1996 | NO_LAYOUT (sem referência) / variantes legadas |

**Tabelas OK (controle, não mexer):** `bens_candidato`, `detalhes_votacao_secao`, `perfil_eleitorado_local_votacao`, `resultados_candidato_uf`, `resultados_partido_uf`. (`bens_candidato` 2014 corrige-se sozinho após `candidatos`.)

---

## 4. Track B — 3 rollups → dbt puro

Reescrever os 3 modelos como agregação in-warehouse via `ref()` sobre a tabela zona publicada (mantendo `config()` atual — schema/alias/partition_by/cluster_by):

```sql
-- br_tse_eleicoes__resultados_candidato_municipio.sql
{{ config( ...unchanged... ) }}
select
    ano, turno, id_eleicao, tipo_eleicao, data_eleicao, sigla_uf,
    id_municipio, id_municipio_tse, cargo, numero_partido, sigla_partido,
    titulo_eleitoral_candidato, sequencial_candidato, numero_candidato, resultado,
    sum(votos) as votos
from {{ ref('br_tse_eleicoes__resultados_candidato_municipio_zona') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
```

- `...__resultados_partido_municipio.sql` — `GROUP BY` 9 cols, `SUM(votos_nominais)`, `SUM(votos_legenda)`.
- `...__detalhes_votacao_municipio.sql` — `GROUP BY` 7 cols, `SUM` de 12 cols, recomputar 4 `safe_divide(...)` (`proporcao_comparecimento/validos/brancos/nulos`).

`ref()` resolve prod no `target=prod` e dev no `target=dev`, **e auto-impõe a ordem zona→municipio** — não usar caminho prod hardcoded.

`resultados_candidato` (all-years) **permanece Python** (`aggregation.py::build_resultados_candidato`): precisa de `norm_candidatos` + concat histórico 1945–1990 + blanking por paridade de ano. Corrige-se ao corrigir `results_mun_zone` + `candidates`.

---

## 5. Track C — Remover Python morto + staging — **só após o gate de paridade passar**

- `aggregation.py`: remover `build_resultados_candidato_municipio`, `build_resultados_partido_municipio`, `build_detalhes_votacao_municipio` de `build_all()` e deletá-las. Manter `build_resultados_candidato`, `build_receitas_comite`, `build_receitas_orgao_partidario`.
- Remover as 3 tabelas do passo de upload para staging.
- Atualizar `DEPENDENCIES.md`: marcar os 3 rollups como "pure dbt (`ref` from `*_municipio_zona`)" e corrigir as linhas `receitas_comite`/`receitas_orgao_partidario`.

---

## 6. Cloud orchestration (Prefect) — seguindo `DEPENDENCIES.md`

Hoje só existem 4 flows agendados (`pipelines/datasets/br_tse_eleicoes/flows.py`: candidatos, bens, despesas, receitas), via factory `_tse_flow()` → `_run_tse_eleicoes()` (`pipelines/crawler/tse_eleicoes/flows.py`), com `upload_to_gcs` + `run_dbt` sequenciais por tabela. As tabelas zona/rollup não têm flow.

**Plano:** adicionar flows por tabela (padrão `_tse_flow` existente) para os produtores zona afetados, e orquestrar a run de rebuild numa **ordem topológica derivada de `DEPENDENCIES.md`**, com `task.submit(..., wait_for=[upstream])` (idiom já usado em `pipelines/utils/execute_dbt_model/flows.py`):

```
Wave 0 (linchpin):  candidatos  ──►  norm_candidatos (Phase-2)
Wave 1 (título-enriched + independentes; dependem da Wave 0 quando carregam titulo):
    bens_candidato, receitas_candidato, despesas_candidato,
    resultados_candidato_secao, resultados_candidato_municipio_zona   (← merge norm)
    partidos, vagas, resultados_partido_municipio_zona,
    detalhes_votacao_{secao,municipio_zona}, perfil_eleitorado_*      (sem merge)
Wave 2 (Phase-3):
    [dbt puro, via ref]  resultados_candidato_municipio,
                         resultados_partido_municipio,
                         detalhes_votacao_municipio
    [Python]             resultados_candidato (rcmz modern + rc_uf hist + norm),
                         receitas_comite, receitas_orgao_partidario
```

Edges `wait_for` a codificar (de `DEPENDENCIES.md`): `candidatos → {bens,receitas,despesas,resultados_candidato_secao,resultados_candidato_municipio_zona}`; cada `*_municipio_zona → seu rollup municipio` (no dbt isso já vem de `ref()`, então o flow do rollup só roda `run_dbt`, sem upload). Os 3 flows de rollup **perdem o passo `upload_to_gcs`** — só `run_dbt`.

---

## 7. Gate de paridade + smoke-test (antes de deletar Python / promover prod)

**1. Paridade old-vs-new (bloqueia Track C e prod):** com o Python dos 3 rollups ainda presente, rodar ambos em dev e comparar via `mcp__databasis__query_bigquery`:
- row counts por `(ano, sigla_uf)` — devem bater exatamente;
- `SUM(votos)` / `SUM(votos_nominais+votos_legenda)` por `(ano, sigla_uf)` — exatos;
- `detalhes_votacao_municipio`: `proporcao_*` a tolerância float.

**2. Smoke-test dos smoking-guns (confirma que a corrupção sumiu):**
- `resultados_candidato_municipio_zona` 1994: `COUNT(votos=0)` cai de ~842k para baseline 1998;
- `candidatos` 1996: `titulo_eleitoral_candidato` NULL% ≈ 0 (era 100%);
- `detalhes_votacao_municipio` 1996: cobertura ~11k municípios / 26 UFs (era 242/19);
- `perfil_eleitorado_municipio_zona` 2008/2016: uma só geração (sem mix código×label);
- `partidos`/`vagas`/`despesas` anos afetados: campos numéricos sem strings/NULL espúrios.

**3. Re-rodar o harness de diagnóstico** (`uv run python -m diagnostics`) e confirmar findings FAIL→OK em `artifacts/findings.json`.

---

## 8. Critical files

- **Builders (Track A):** `code/python/sub/{candidates,results_mun_zone,voting_details_mun_zone,voter_profile_mun_zone,parties,vacancies,campaign_finance,voter_profile_section,voting_details_state,results_section}.py`
- **Loader compartilhado (novo):** `code/python/utils/layout.py` (reusa `diagnostics/.../tier2_leiame.py::load_layout` + artifacts `diagnostics/artifacts/layouts/*.json`)
- **Helper a ajustar:** `code/python/utils/helpers.py:16` (`read_raw_csv` — opção header-based)
- **dbt (Track B):** `models/br_tse_eleicoes/br_tse_eleicoes__{resultados_candidato_municipio,resultados_partido_municipio,detalhes_votacao_municipio}.sql`
- **Python morto (Track C):** `code/python/aggregation.py`; passo de staging upload; `code/DEPENDENCIES.md`
- **Prefect:** `pipelines/datasets/br_tse_eleicoes/{flows.py,schedules.py}`, `pipelines/crawler/tse_eleicoes/flows.py`, `pipelines/utils/tasks.py` (`upload_to_gcs`, `run_dbt`)

---

## 9. Verification

```bash
# 0. Rodar harness de diagnóstico antes/depois (espera FAIL→OK)
cd models/br_tse_eleicoes/code && uv run python -m diagnostics

# 1. dbt: materializar os 3 rollups em dev e provar ordem ref()
uv run dbt run --select br_tse_eleicoes__resultados_candidato_municipio \
  br_tse_eleicoes__resultados_partido_municipio \
  br_tse_eleicoes__detalhes_votacao_municipio --target dev
uv run dbt ls --select +br_tse_eleicoes__resultados_candidato_municipio   # prova ordem zona→municipio
uv run dbt test --select br_tse_eleicoes__resultados_candidato_municipio \
  br_tse_eleicoes__resultados_partido_municipio br_tse_eleicoes__detalhes_votacao_municipio

# 2. Gate de paridade + smoke-test via mcp__databasis__query_bigquery (queries acima)
# 3. Só após gate verde: deletar Python (Track C), atualizar DEPENDENCIES.md
# 4. Cloud: disparar flows de rebuild na ordem topológica (wait_for); validar smoking-guns em prod no pool k8s
```

Notas: rodar dbt/pytest via `uv run` (`.venv` puro é Prefect1/Pydantic1 stale); validação BQ prod só roda in-pod (creds locais não têm jobs.create em prod); `sigla_uf='BR'`→NULL é convenção BD.
