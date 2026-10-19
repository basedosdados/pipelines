# Documentação do Conjunto de Dados: br_anatel_telefonia_movel

Este documento centraliza o contexto dos dados de telefonia móvel da Anatel (Serviço Móvel Pessoal — SMP) e as decisões de engenharia que explicam por que o pipeline é como é. Referência para futuros mantenedores.

---

## Sobre o sistema

Dados publicados **mensalmente** pela Anatel (Gerência PRUV) sobre acessos e densidade do SMP.

- **Fonte (dados.gov.br):** https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp
- **API usada pelo pipeline:** `https://dados.gov.br/api/publico/conjuntos-dados/acessos-autorizadas-smp` (requer cookies/headers em `constants.py`)
- **Portal Anatel:** https://www.gov.br/anatel/pt-br/dados/acessos/telefonia-movel

O conjunto tem 4 tabelas: `microdados` (acessos por município/operadora/tecnologia), `densidade_brasil`, `densidade_uf`, `densidade_municipio`. Granularidade: nacional, UF e municipal.

**Densidade** = acessos em operação (chips ativos) por **100 habitantes** (nas demais modalidades da Anatel é por 100 domicílios — aqui é habitantes).

---

## Particularidades da fonte (pontos críticos)

### 1. A fonte revisa dados retroativamente

Documentação oficial de metadados da Anatel (SMP), Obs. 2: *"as planilhas foram elaboradas com base nas informações fornecidas pelas prestadoras. Se necessário, os dados poderão sofrer correções no futuro."*

Consequências observadas:
- **Degraus no total de acessos** entre anos (ex.: 2022→2023 cai à metade) — são revisões/metodologia, não erro de ingestão.
- **Snapshots "encolhem" meses antigos:** o arquivo baixado hoje pode trazer detalhe completo só dos ~2 meses mais recentes e versões reduzidas de meses passados. O histórico detalhado é preservado no **staging** porque o upload usa `dump_mode="append"` (acumula sem duplicar). **Nunca** fazer full refresh assumindo que o arquivo atual tem todo o histórico.

### 2. Granularidade UF/municipal só existe a partir de 2010

A densidade por UF e por município só passa a existir em **2010**. Para **2005-2009**, as linhas de UF carregam `densidade = 0` (placeholder — não é densidade real; o nacional do mesmo período é 37-140). Ver detalhes na tabela `densidade_uf` abaixo.

### 3. Formato numérico do `acessos` mudou em 2025

A partir de **2025-01** a Anatel emite `acessos` como **float** (`"1.0"`) em vez de inteiro (`"1"`). Como `safe_cast(... as int64)` não parseia `"1.0"`, os valores viram NULL silenciosamente. Ver tabela `microdados` abaixo.

### 4. Arquivo de densidade é único, dividido por rótulo

As 3 tabelas de densidade saem do **mesmo** `Densidade_Telefonia_Movel.csv`, filtrado pela coluna `Nível Geográfico Densidade` (renomeada para `geografia`) nos valores `"Brasil"`, `"UF"`, `"Municipio"` (`clean_csv_brasil/uf/municipio` em `utils.py`).

**Robustez (aplicada):** os filtros de geografia foram tornados robustos a acento/caixa via `normalize_label` (`utils.py`), usado nos 3 `clean_csv_*` e no poll (`tasks.py`). O glossário grafa "Município" (acentuado), mas o dado vem sem acento — se a Anatel "corrigir" a grafia numa revisão, o filtro normalizado (sem acento, minúsculo, sem espaços) continua casando, evitando quebra silenciosa.

---

## Estrutura dos flows e o poll

Cada tabela tem um flow próprio, agendado num dia da semana, e todos chamam `_run_anatel_telefonia_movel`, no mesmo `flows.py`. O código fica todo em `pipelines/datasets/br_anatel_telefonia_movel/`, sem diretório em `pipelines/crawler/`.

**Poll por geografia:** as 3 densidades saem do mesmo arquivo, então `get_max_date_in_table_microdados` (`tasks.py`) calcula a data máxima **filtrando pela geografia da tabela** — não o max do arquivo inteiro.

**"Última atualização na fonte original" gravada antes da carga:** quando o poll acha mês novo, `commit_source_update_task` grava o `RawDataSource.Update` logo em seguida, antes de baixar e materializar. Assim, se o flow falhar no meio, o metadado da fonte continua mostrando que a Anatel publicou dado novo. Isso não trava a tabela, porque o poll compara o mês da fonte com o último mês da tabela (`compare_against="coverage"`), que só avança quando a materialização termina (PR #1783).

**Histórico do bug (contexto):** antes, a função retornava `df["data"].max()` do arquivo inteiro, então o `source_max_date` de cada densidade refletia a data de *outra* geografia (UF/Município), não a própria; e o poll era **eager** (gravava o `Update` antes de materializar). Uma falha de materialização carimbava o ponteiro adiantado e travava a tabela — foi o que travou a `densidade_brasil`.

**Parâmetros:** em prod, rodar com defaults; `force_run=True` força a ingestão de um mês, útil para destravar/backfill.

---

## Tabelas e particularidades

### br_anatel_telefonia_movel__microdados

Acessos por município, operadora, tecnologia, modalidade, tipo de pessoa e produto.

**Problema identificado — `acessos` 100% NULL de 2025-01 a 2026-03 (15 meses):**
- Causa: a fonte passou a emitir `acessos` como float (`"1.0"`) em 2025; `safe_cast(acessos as int64)` nulifica.
- **Fix (APLICADO):** o cast no modelo é `safe_cast(safe_cast(acessos as float64) as int64)`. Retrocompatível (`"1"`→1, `"1.0"`→1, `"null"`→NULL). Como a tabela é `materialized="table"` e o staging guarda os `"1.0"` crus, um `dbt run` recupera os 15 meses **sem rebaixar da Anatel**. Validado em dev (`dbt run`+`dbt test` OK); prod via PR.
- Defesa extra: normalizar `acessos` também no `clean_csv_microdados` (o único numérico que não é tratado no pandas).

**Partição (corrigida):** o `range` da partição por `ano` foi ajustado para `end: 2031` (2026 + 5, convenção BD) — antes estava em 2023, deixando os anos ≥2023 em partição residual (degradava custo/pruning).

### br_anatel_telefonia_movel__densidade_brasil

Densidade nacional (1 linha por mês).

**Problema identificado — tabela travada em 2026-01** (as outras densidades foram até 2026-03):
- **Não** é ausência na fonte — o arquivo tem `"Brasil"` para todos os meses. É o **bug do poll** (ver seção acima): o ponteiro do brasil foi carimbado adiantado e o flow passou a retornar antes de materializar (falhas de junho/2026).
- **Fix permanente (aplicado):** poll por geografia + deferido (ver seção acima), que impede o ponteiro de avançar sem materialização bem-sucedida.
- **Recuperação da tabela já travada (pós-deploy):** disparar uma vez com `force_run=True` — o ponteiro já foi queimado antes do fix, então o `force_run` materializa 2026-02..05 e ressincroniza.

### br_anatel_telefonia_movel__densidade_uf

Densidade por UF.

**Pontos de atenção (confinados a 2005-2009, não bloqueantes — mantidos por decisão):**
- **1296 zeros:** placeholders legítimos — granularidade UF só existe de 2010 em diante (antes era por Região do PGO). `densidade = 0` para todas as UFs em 2005-2008 é ausência de desagregação, não densidade real. **Decisão: mantidos**, pois a BD preserva o histórico da série (a base publica dados de 2005+).
- **48 linhas com `sigla_uf` vazio** (1 por mês de 2005-2009): resíduo capturado pelo filtro de geografia — sem UF nem valor. **Decisão: mantidas por mínimo-toque** (não alterar a tabela já publicada pela BD). Não são "dado histórico" no sentido estrito; são resíduo tolerado.
- De 2010 em diante: 27 UFs por mês, sem nulos/zeros. Siglas corretas (AC…TO).

### br_anatel_telefonia_movel__densidade_municipio

Densidade por município (5570 municípios por mês, 2019+). Sem problemas de integridade; atraso eventual vs. fonte é o lag normal do poll (1 mês/run).

---

## Glossário das colunas (fonte: metadados oficiais da Anatel)

| Coluna | Descrição |
|--------|-----------|
| Ano / Mês | Período de referência do registro |
| UF | Nome da UF; `"Brasil"` nos registros nacionais |
| Município | Nome do município; `"Brasil"` no nacional, sigla da UF no nível UF |
| Código IBGE | Código do município (7 dígitos), só em registros municipais |
| Densidade | Acessos por 100 habitantes |
| Nível Geográfico Densidade | Granularidade: `"Brasil"`, `"UF"` ou `"Municipio"` |

---

## Referências

- Conjunto no dados.gov.br: <https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp>
- Portal Anatel (Telefonia Móvel): <https://www.gov.br/anatel/pt-br/dados/acessos/telefonia-movel>
- Metadados e glossário das colunas: documentação oficial de metadados da Anatel (SMP)
