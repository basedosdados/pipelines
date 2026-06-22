# Decisões de migração — `br_tse_eleicoes`

Registro consolidado das decisões tomadas na refatoração (parsing header-based +
rollups dbt + orquestração Prefect 3). Documentos relacionados:
[`PROPOSAL_dbt_rollups.md`](PROPOSAL_dbt_rollups.md), [`DEPENDENCIES.md`](DEPENDENCIES.md),
[`DIAGNOSIS.md`](DIAGNOSIS.md), [`diagnosis/README.md`](diagnosis/README.md).

Legenda de status: ✅ implementada · ⏳ pendente · ⚠️ a revisar no smoke-test.

---

## 1. Arquitetura geral

### D1 — Builders permanecem em `models/code/python`; harness é o gate ✅
A correção header-based e o harness de diagnóstico (`diagnostics/`) ficam em
`models/br_tse_eleicoes/code/python`. O harness (tier1 AST + tier3 layout) é o
gate de validação por etapa. **Por quê:** mover a lógica quebraria os paths que
o harness audita; o harness foi projetado para validar tanto o estado posicional
quanto o nomeado.

### D2 — `utils/layout.py` lê os artifacts de layout diretamente ✅
`resolve_columns(df, family, ano)` / `layout_columns(family, ano)` leem os JSONs
em `diagnostics/artifacts/layouts/{family}_{ano}.json` **diretamente** (sem
importar o pacote `diagnostics`). **Por quê:** desacopla o runtime do harness e
garante que o rename em runtime e o `tier3._check_named` consumam o **mesmo**
artifact → consistência por construção. Layouts atualizáveis via
`python -m diagnostics run --tier 2`.

### D3 — Rename usa o layout cacheado, não o header do arquivo ✅
Mesmo quando o arquivo tem header real (1994+), `resolve_columns` renomeia
`vN→nome oficial` pelo **layout cacheado**, não pelo header lido. **Por quê:**
evita divergência entre runtime (header real) e harness (layout cacheado). Se o
TSE mudar um layout, reacquirir os artifacts (tier 2) atualiza ambos juntos.

---

## 2. Padrão de conversão dos builders

### D4 — Seleção por nome oficial TSE, dict literal nomeado ✅
Trocar `cols = {"v26": "situacao"}` por `cols = {"NR_PARTIDO": "numero_partido"}`,
mantendo o nome de variável `cols`/`keep_cols`/`col_map`. **Por quê:** o
`tier1_audit` reclassifica o site `positional→named` automaticamente; o
`tier3._check_named` valida que toda chave existe no layout do ano. Lookup por
nome **não pode desalinhar** → existência da chave prova a correção do
mapeamento (a correção semântica é julgamento humano, feito uma vez).

### D5 — Condições de ano com tupla literal inline (não constante de módulo) ✅
Usar `if ano in (1998, 2000, ...):` e não `if ano in _CONST:`. **Por quê:** o
AST do tier1 avalia a condição com `ano` no ambiente; uma constante de módulo dá
`NameError` → o site é marcado como "variante" cobrindo todos os anos → WARNs
espúrios de `MISSING_KEY`. A tupla inline resolve os anos exatos por branch.

### D6 — Múltiplas gerações de layout = múltiplos dicts literais por branch ✅
Quando o nome oficial difere entre gerações, escrever um dict literal por
geração (cada um vira um site `named` validado). O filtro `available = {k:v ... if
k in df.columns}` descarta chaves ausentes legitimamente.

---

## 3. Decisões semânticas por tabela

### D7 — `candidatos`: três gerações de layout ✅
`_parse_schema` tem 3 branches por nome oficial:
- **FULL** {1998–2012, 2016}: `DS_DETALHE_SITUACAO_CAND`→situacao, `NM_EMAIL`→email,
  `DS_NACIONALIDADE`→nacionalidade, `NM_MUNICIPIO_NASCIMENTO`→municipio_nascimento.
- **REDUCED** {1994,1996,2014,2018,2020,2022}: `DS_SITUACAO_CANDIDATURA`→situacao,
  `DS_EMAIL`→email; nacionalidade e municipio_nascimento **ausentes** na fonte
  (federação inserida, bloco demográfico removido).
- **2024**: reduced no arquivo principal; nacionalidade/municipio_nascimento/situacao
  vêm do arquivo **complementar** (`DS_SITUACAO_CANDIDATO_TOT`→situacao via merge).

**Por quê:** preserva a semântica de cada geração; não altera anos hoje-OK.

### D8 — `resultados_candidato_municipio_zona.votos` unificado para `QT_VOTOS_NOMINAIS` ⚠️
**Decisão:** usar `QT_VOTOS_NOMINAIS` para **todos** os anos (dict único, sem branch).

**Contexto:** o código posicional antigo lia, nos anos OK, `QT_VOTOS_NOMINAIS`
(2002–2016, 2024) **mas** `QT_VOTOS_NOMINAIS_VALIDOS` em 1998/2000 (artefato da
posição `v42`). Ambos os nomes existem em todos os anos. Nos anos FAIL (1994,
1996, 2016, 2018–2022) a posição lia colunas erradas (ex.: 1994 `v40`=`SG_FEDERACAO`
→ os ~842k votos=0).

**Por quê `QT_VOTOS_NOMINAIS`:** dá uma coluna `votos` coerente em toda a série
(usuários comparam anos); `_VALIDOS` (votos nominais válidos) é um subconjunto e
era um acidente posicional, não uma escolha.

**Efeito colateral:** muda os valores de **1998/2000** (de "nominais válidos" para
"nominais"). Como o escopo desta rodada é dev+harness, validar no smoke-test e na
revisão antes de promover a prod. Reverter é trivial (branch de 2 anos) se a
revisão preferir preservar 1998/2000.

### D9a — `detalhes_votacao_municipio_zona`: layout idêntico, 2 branches por método ✅
Layout `detalhe_votacao_munzona` é idêntico em todos os anos (n=47); o FAIL de
1996/2000–2016 vinha de o grupo `elif` ler posições erradas. Branches: `ano<=2016`
(método 1994/1998: votos_validos=nominais+legenda, votos_nulos=soma de
QT_VOTOS_NULOS+TECNICOS+ANULADOS_APU_SEP) e `>=2018` (totais diretos
QT_TOTAL_VOTOS_VALIDOS/QT_TOTAL_VOTOS_NULOS). **Não altera anos OK.**

### D9b — `vagas`: layout idêntico, 2 branches só pela coluna de vagas ✅
`consulta_vagas` idêntico (n=15); o `<=2012` lia cargo←DT_POSSE, vagas←SG_UF.
Branch só troca `QT_VAGAS` ({1998–2012,2016}) vs `QT_VAGA` (resto), que nunca
coexistem. `id_eleicao`/`data_eleicao` agora mapeados de CD_ELEICAO/DT_ELEICAO.

### D9c — `partidos`: 3 gerações, split is_federal eliminado ✅
`consulta_coligacao`: 1990 (headerless, nomes legados `DESCRICAO_ELEICAO`/
`TIPO_LEGENDA`/…), 1994–2016 (n=23), 2018–2024 (n=28, bloco federação +
`DS_SITUACAO`→situacao_legenda). O split federal/municipal foi removido: `SG_UE`
mapeia sempre id_municipio_tse e, em anos federais, é alfabético → vazio após
destring (mesmo resultado do código antigo).

### D9d — `perfil_eleitorado_municipio_zona`: colunas CD_ (códigos) preservadas ✅
2 gerações: n=21 (bloco `CD_MUN_SIT_BIOMETRICA`, anos {1994–2006,2018}) e n=28
(sem biometria, blocos raça/identidade/quilombola/libras inseridos). genero/
estado_civil/grupo_idade/instrucao usam as colunas **CD_** (código), batendo com
os anos OK e a geração dominante em prod (`genero ∈ 0/2/4`, conforme DIAGNOSIS Q5)
— **não** os rótulos DS_. Preserva anos OK; corrige o desalinhamento dos n=28.

### D9e — `despesas_candidato` 2014: header em português ✅
`prestacao_despesas_2014` tem header legível em português (n=25, ex.
`"Valor despesa"`, `"Sigla  Partido"` com espaço duplo); o map posicional antigo
levava valor_despesa para `"Cod setor econômico do fornecedor"`. Mapeado pelos
nomes exatos do header.

### D9 — `resultados_partido_municipio_zona.votos_*`: branch forçado por disponibilidade ✅
- {2000–2014}: `QT_VOTOS_NOMINAIS`/`QT_VOTOS_LEGENDA`.
- {1994,1996,1998,2016,2018–2024}: `QT_VOTOS_NOMINAIS_VALIDOS`/`QT_TOTAL_VOTOS_LEG_VALIDOS`.

**Por quê:** ao contrário do candidato, aqui **só uma** variante existe por ano
(o nome plano não existe em 1994/96/98/2016+, e o `_VALIDOS` não existe em
2000–2014). A escolha é imposta pela fonte, não uma preferência. A mistura
válido/nominal entre anos é inerente à fonte.

---

## 4. Orquestração Prefect 3 (Track D)

### D10 — `pipelines/br_tse_eleicoes/` contém **só orquestração** ✅
Wrappers no padrão cookiecutter Prefect 3 (`constants/utils/tasks/flows/schedules`)
que invocam os builders de `models/code/python` via subprocess (`run_build_step`).
A correção de parsing e o harness **não** migram. **Por quê:** decisão do usuário;
mantém o gate de validação intacto e evita duplicação.

### D11 — Deployments por tabela + `run_deployment` encadeado ✅
Um `_tse_table_flow` por tabela → um deployment cada (`deploy_flows.py --all` varre
`pipelines/**`). Orquestrador `br_tse_eleicoes__refresh` dispara cada tabela via
`run_deployment` com `.submit(wait_for=[...])` nas waves de `DEPENDENCIES.md`.
**Por quê:** decisão do usuário; alinha com a topologia de CI Prefect 3 existente.

### D12 — Novo diretório, antigos preservados ✅
Criar `pipelines/br_tse_eleicoes/` sem remover `pipelines/datasets/br_tse_eleicoes/`
nem `pipelines/crawler/tse_eleicoes/`. **Por quê:** decisão do usuário; migrar/limpar
depois.

### D13 — Três perfis de flow ✅
- **producer**: build (python) → normalize → `upload_to_gcs` → `run_dbt`.
- **dbt_rollup**: **só** `run_dbt` (o `ref()` resolve o upstream; sem upload).
- **python_agg**: aggregate (python) → `upload_to_gcs` → `run_dbt`.

### D14 — Caveat: `normalize` (Phase-2 global) ⚠️
`normalize` constrói `norm_candidatos` + escreve os parquets particionados de
**todas** as tabelas de uma vez. Hoje roda dentro de cada flow producer (cada
deployment é auto-contido), o que é redundante num `__refresh` completo.
**TODO** documentado em `flows.py`: extrair um deployment dedicado de
build+normalize do qual os producers dependam.

---

## 5. Escopo e validação

### D15 — Esta rodada: dev + harness apenas ✅
Validação: harness (tier1/tier3 FAIL→OK), rebuild em dev, gate de paridade
old-vs-new e smoke-tests via `query_bigquery`. **Sem** promoção a prod e **sem**
execução real dos flows Prefect nesta rodada.

### D16 — Tabelas de controle (não mexer) ✅
`bens_candidato`, `detalhes_votacao_secao`, `perfil_eleitorado_local_votacao`,
`resultados_candidato_uf`, `resultados_partido_uf` estão OK no baseline — não
converter.

---

## Histórico de validação (harness)

| Marco | tier3 |
|---|---|
| Baseline (antes) | FAIL=62, WARN=28, NO_LAYOUT=4, OK=162 |
| `candidatos` convertido | OK=17, 0 FAIL/WARN |
| `resultados_candidato_municipio_zona` | OK=16, 0 FAIL |
| `resultados_partido_municipio_zona` | OK=16, 0 FAIL |
| `detalhes_votacao_municipio_zona` | OK=16, 0 FAIL |
| `partidos` | OK=17, 0 FAIL |
| `vagas` | OK=16, 0 FAIL |
| `perfil_eleitorado_municipio_zona` | OK=16, 0 FAIL |
| `despesas_candidato` 2014 | OK=12, 0 FAIL |
| **Track A completo (global)** | **FAIL=0** (62→0), WARN=28, NO_LAYOUT=4, OK=218; tier1 0 issues |

WARN/NO_LAYOUT remanescentes (idênticos ao baseline, aceitáveis): `detalhes_votacao_uf`
1945–1990 (headerless), `perfil_eleitorado_secao` 2008–2022 (layout baixa confiança),
`resultados_candidato_secao` 1998/2008, `receitas_candidato` 2014 sup. (variante não verificável).
