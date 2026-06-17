# Onboarding — Migração dbt-core → dbt Fusion

Handoff para retomar a migração numa próxima iteração. Lê isto primeiro, depois
os dois docs de detalhe:

- **Plano de migração:** `docs/dbt-fusion-migration.md` (o que mudou e por quê)
- **Plano de testes:** `docs/dbt-fusion-test-plan.md` (como validar, com resultados já executados)

---

## 1. Objetivo e estratégia

Migrar o engine dbt do `dbt-core` (Python) para o **dbt Fusion engine** (Rust),
de forma **faseada e reversível**:

- Fusion convive com o dbt-core atrás de um switch de runtime (`DBT_ENGINE`).
- dbt-core continua sendo o engine de **produção** até o adapter BigQuery do
  Fusion ir a **GA** (hoje está em **Preview/beta**; engine licenciado **ELv2**).
- Promoção a prod = setar `DBT_ENGINE=fusion` no worker. Rollback = remover a env.

Duas restrições que moldaram tudo:
1. Fusion **não** suporta `dbtRunner` (API programática) → o runtime virou `subprocess`.
2. Adapter BigQuery beta + licença ELv2 → cutover total é prematuro.

---

## 2. Estado atual (o que já foi feito)

Tudo em **working tree** (nada commitado ainda). `git status`:

| Arquivo | Mudança |
|---------|---------|
| `dbt_project.yml` | `require-dbt-version: [">=1.8.0", "<3.0.0"]` |
| `packages.yml` | `dbt_utils` `1.1.1 → 1.3.0` (compatível Fusion) |
| `Dockerfile` | instala binário Fusion (`ARG DBT_FUSION_VERSION`), coexiste com core; `dbt`=core, `dbtf`=fusion |
| `.github/workflows/cd-staging.yaml` | novo job `fusion-checks` (paralelo, `continue-on-error`) roda `dbtf test` em PRs `test-dev-model` |
| `pipelines/utils/execute_dbt_model/engine.py` | **novo** — `run_dbt_command()` despacha por `DBT_ENGINE` (core=`dbtRunner`, fusion=`subprocess dbtf`) |
| `pipelines/utils/execute_dbt_model/flows.py` | usa `run_dbt_command()` |
| `pipelines/utils/tasks.py` | usa `run_dbt_command()` |
| `CONTRIBUTING.md` | seção Fusion (instalação, dois engines, venv, ELv2) |
| `docs/dbt-fusion-*.md` | plano de migração + plano de testes |

Validado neste host: `py_compile`, paridade de `build_cli_args`, parse YAML,
import de `flows.py`/`tasks.py`, e `dbtf deps` (instala `dbt_utils 1.3.0` ✅).

---

## 3. 🚧 BLOQUEADOR principal — Fase 0 (faça isto primeiro)

`dbtf parse` no projeto inteiro **falha com 2219 erros** hoje (reproduzido em
`feat/dbt-fusion-parse-fixes`; `dbtf deps` OK, instala `dbt_utils 1.3.0`).
Investigação rastreou todos os erros a **4 buckets** — um mecânico, três manuais.

### Bucket 1 — `dbt0102` (2164) · mecânico, automatizado
Args de teste genérico no formato antigo (`combination_of_columns`, `at_least`,
`field`/`to`, `values`, `expression`, `ignore_values`,
`proportion_allowed_failures`…) no topo do teste; o Fusion exige aninhados sob
`arguments:`. Espalhados por `models/**/schema.yml`.
→ **Correção:** `uvx dbt-autofix deprecations` (sem edição manual).

### Bucket 2 — `dbt1501` (12) · 3 correções de nome
Macros/testes "desconhecidos" são **nomes errados**, não implementações ausentes:

| Referência | Arquivo | Nome real | Correção |
|------------|---------|-----------|----------|
| `set_dalake_project` | `br_bd_diretorios_data_tempo__trimestre.sql:10` | `set_datalake_project` | corrigir typo (1 spot) |
| `custom_dictionaries` | `br_ibge_censo_2022/schema.yml:61` | `custom_dictionary_coverage` | renomear (args batem: `dictionary_model` + `columns_covered_by_dictionary`) |
| `english_tables_dictionary_coverage` (×10) | `world_iea_timss/schema.yml` | `custom_dictionary_coverage_eng` | renomear (×10) |

### Bucket 3 — `dbt1060` (43) · bugs latentes que o dbt-core ignorava em silêncio
Os testes/configs afetados **nunca rodaram** no core. Corrigir muda
comportamento → revisar caso a caso:
- **`where:` sob `config:` de coluna** (~36) em `br_ms_cnes`, `br_inep_ideb`,
  `br_me_cnpj` — `where` não é chave de config de coluna; pertence ao `config`
  de cada *teste*. Os filtros incrementais `__most_recent_*__` foram descartados.
- **`test:` (singular)** em vez de `tests:` (2 colunas) em `br_inep_ideb:202/240`
  — os `accepted_values` nunca rodaram.
- **`materialization="..."`** em vez de `materialized=` em
  `br_denatran_frota__municipio_tipo.sql` e `__uf_tipo.sql` — esses modelos
  **nunca foram materializados como incremental/table como pretendido.**
- **`tag=["refazer"]`** em vez de `tags=` em
  `br_ibge_censo_2022__caracteristica_domicilio...tipo_domicilio.sql`.

### Bucket 4 — warnings não-bloqueantes (anotar, não corrigir agora)
`dbt1080` (compat de versão do `dbt_utils`), `dbt1087` (4× `ref()` não resolvido
para `br_bcb_sicor/sicar` dicionario), `dbt1089`. Não interrompem o parse.

### Ordem de execução da Fase 0
0. **Baseline core** — criar `/app/dbt_packages` (`sudo mkdir -p /app/dbt_packages
   && sudo chown -R "$USER" /app/dbt_packages`), `uv run dbt deps`, `uv run dbt
   parse` e guardar o resultado (guardrail anti-regressão).
1. **Bucket 1** — `uvx dbt-autofix deprecations`; reparsear até `dbt0102` → 0.
2. **Bucket 2** — as 3 correções de nome.
3. **Bucket 3** — os bugs latentes, com julgamento por caso.
4. **Gate Fase 0:** `dbtf parse` com **0 erros** E `uv run dbt parse` (core) sem
   regressão. Só então seguir para materialização (Fases A/B/C).

---

## 4. Próximos passos (em ordem)

1. **Fase 0** — migrar formato dos `schema.yml` (autofix + manual) → `dbtf parse` limpo.
2. **Validação local (Fases A/B)** — `run`/`test` de 1-2 modelos no core e no Fusion em `basedosdados-dev`; comparar row count/schema.
3. **Worker dev (Fase C)** — build da imagem dev com Fusion → deploy `run_dbt_model_flow` no pool `basedosdados-dev` → rodar C1 (core, fallback), C2 (`DBT_ENGINE=fusion`), C3 (falha proposital).
4. **Commit + PR** — em branch (não `main`); sem `Co-Authored-By` (convenção do repo).
5. **Gate de promoção** — só virar prod quando o adapter BigQuery do Fusion for GA e A/B/C estiverem verdes.

---

## 5. Fatos do ambiente / gotchas

- Versões: dbt-core `1.8.8` (`uv run dbt`), dbt-fusion `2.0.0-preview.175` (`dbtf` → `~/.local/bin/dbt`).
- Credenciais dev: `~/.basedosdados/credentials/staging.json`; faça `source .env` (senão `PREFECT_API_*` faltam → httpx ConnectError).
- `packages-install-path: /app/dbt_packages` é caminho **de container** → local quebra:
  - dbt-core 1.8: sem override → `sudo mkdir -p /app/dbt_packages && sudo chown -R "$USER" /app/dbt_packages`.
  - Fusion: `export DBT_PACKAGES_INSTALL_PATH="$PWD/dbt_packages"`.
- `dbtf deps` gera `package-lock.yml` na raiz (gitignore? não — limpar ou decidir versionar).
- Materializações escrevem em `basedosdados-dev` (compartilhado) — combine antes de rodar.
- Validação BQ de **prod** só roda no pod k8s (creds locais não têm jobs.create em prod).

---

## 6. Onde a chave do runtime mora

`pipelines/utils/execute_dbt_model/engine.py`:
- `DBT_ENGINE` (default `core`) → escolhe `_run_core` (dbtRunner) ou `_run_fusion` (subprocess `dbtf`).
- `DBT_FUSION_BIN` (default `dbtf`) → nome/caminho do binário.
- No worker, setar `DBT_ENGINE` via env do work pool (`Base Job Template`) ou `job_variables` do deployment.
