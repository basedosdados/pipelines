# Documentação do Conjunto de Dados: br_anatel_telefonia_movel

Este documento centraliza o contexto dos dados de telefonia móvel da Anatel (Serviço Móvel Pessoal — SMP) e as decisões de engenharia que explicam por que o pipeline é como é. Referência para futuros mantenedores.

---

## Sobre o sistema

Dados publicados **mensalmente** pela Anatel (Gerência PRUV) sobre acessos e densidade do SMP.

- **Fonte (dados.gov.br):** https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp
- **API usada pelo pipeline:** `https://dados.gov.br/api/publico/conjuntos-dados/acessos-autorizadas-smp`, só para descobrir a URL do ZIP (requer cookies/headers em `constants.py`)
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

As 3 tabelas de densidade saem do **mesmo** `Densidade_Telefonia_Movel.csv`, filtrado pela coluna `Nível Geográfico Densidade`, que traz `"Brasil"`, `"UF"` ou `"Municipio"`.

O nível de cada tabela fica em `DENSITY_GEOGRAPHIC_LEVEL` (`constants.py`), já normalizado. Antes da comparação, a coluna passa pela `normalize_label` (`utils.py`), tanto no poll quanto na limpeza. O glossário grafa "Município", com acento, mas o dado vem sem. Como a normalização tira os acentos e os espaços das pontas e passa tudo para minúsculas, o filtro continua casando se a Anatel mudar a grafia numa revisão.

### 5. A Anatel regrava semestres anteriores

A Anatel gera o ZIP inteiro de novo a cada publicação, e o conteúdo de semestres anteriores pode mudar. O flow lê só o semestre mais recente, então a revisão de um semestre anterior não chega à tabela.

---

## Download

`get_zip_url` (`utils.py`) consulta a API do dados.gov.br, que lista os recursos do conjunto. O recurso de formato `CSV` aponta para um ZIP único da Anatel, de cerca de 3 GB, que traz:

| Arquivo | Conteúdo | Lido por |
| --- | --- | --- |
| `Acessos_Telefonia_Movel_2026_2S.csv` (um por semestre, de 2019 em diante) | microdados do semestre, de 82 a 245 MB compactados | `microdados`, só o semestre mais recente |
| `Densidade_Telefonia_Movel.csv` | densidade nos três níveis geográficos | as 3 densidades |
| `Acessos_Telefonia_Movel_Total.csv` | total de acessos por mês | nenhuma tabela |

O flow não baixa o ZIP inteiro. O servidor da Anatel aceita o cabeçalho HTTP `Range`, e a `HttpRangeFile` (`utils.py`) faz o ZIP remoto funcionar como um arquivo comum, com `read`, `seek` e `tell`, e pede ao servidor só os bytes que o `zipfile` lê. Assim, `download_csv_from_zip` lê o índice do ZIP e extrai só o CSV da tabela para `/tmp/data/input/`, em requisições de até 8 MiB (o `buffer_size` do `BufferedReader`). No semestre de 82 MB compactados, são 11 requisições de dados, em cerca de 10 s.

`get_csv_file_from_zip` escolhe o arquivo pelo nome. As densidades leem o `Densidade_Telefonia_Movel.csv`. O `microdados` lê o semestre mais recente, que é o `max()` dos nomes: como o formato do nome é fixo, a ordem alfabética coincide com a do tempo.

---

## Limpeza

A limpeza usa polars em modo lazy (`scan_csv` e `sink_csv`) e lê todas as colunas como texto (`infer_schema_length=0`). Os tipos vêm do `safe_cast` dos modelos dbt. Os CSVs usam `;` como separador e começam com BOM UTF-8, que o polars descarta.

- **`microdados`** (`clean_csv_microdados`): grava um `data.csv` por mês, em pastas como `ano=2026/mes=8/`, com o mês sem zero à esquerda. As colunas `ano` e `mes` ficam só no caminho, fora do arquivo.
- **Densidades** (`clean_csv_densidade`): filtra o nível, renomeia as colunas (`RENAME_COLUMNS_DENSIDADE`), troca a vírgula decimal da densidade por ponto e grava a série inteira num arquivo só, com o nome da tabela (`densidade_uf.csv`, por exemplo).

A staging é uma tabela externa que lê os arquivos do bucket, e o upload só substitui o arquivo de mesmo caminho. Se o caminho ou o nome mudar (`mes=08`, ou `data.csv` nas densidades), o arquivo antigo continua no bucket e os dados aparecem duas vezes na staging.

O pico de memória da limpeza cresce com o número de threads do polars. Medição no CSV de um semestre com dois meses (602 MiB), com a memória limitada por `systemd-run -p MemoryMax=...`:

| Threads do polars | Limite de memória em que a limpeza termina |
| --- | --- |
| 32 | 2 GB (morre com 1 GB) |
| 4 | 1 GB |
| 2 | 512 MB |

Por padrão, o polars usa uma thread por CPU disponível. O flow não define `POLARS_MAX_THREADS`, a variável de ambiente que limita esse número.

---

## Estrutura dos flows e o poll

Os flows seguem o modelo em estágios (`CheckThenExtractLoadPipeline`, em `pipelines/utils/stage_dispatch.py`). Cada tabela tem dois, em `flows.py`:

| Flow | Agendamento | O que faz | Memória |
| --- | --- | --- | --- |
| `br_anatel_telefonia_movel_<tabela>_check_update` | um dia da semana, às 16h30 | baixa o CSV da tabela, calcula o mês mais recente e compara com a cobertura da tabela; se a fonte estiver à frente, grava o `RawDataSource.Update` e dispara o `download` | padrão do deploy para `check_update` (1Gi) |
| `br_anatel_telefonia_movel_<tabela>_download` | disparado pelo `check_update` | baixa o CSV de novo, limpa e sobe para a staging de dev; o `build_and_promote` genérico materializa, testa e promove para prod | padrão do pool (4Gi e 2 CPUs) |

O `check_update` baixa o CSV porque o mês mais recente da fonte só aparece dentro dele. `get_max_date_in_table_microdados` (`tasks.py`) calcula esse mês no CSV do semestre, para o `microdados`, e no CSV de densidade **filtrado pela geografia da tabela**, para as densidades. No microdados, o `build_and_promote` promove para prod só as partições `ano=/mes=` gravadas no run; nas densidades, que não têm partição, promove a tabela inteira.

O código fica todo em `pipelines/datasets/br_anatel_telefonia_movel/`, sem diretório em `pipelines/crawler/`.

**"Última atualização na fonte original" gravada antes da carga:** quando o `check_update` acha mês novo, grava o `RawDataSource.Update` antes de disparar o `download`. Se o `download` falhar, o metadado da fonte continua mostrando que a Anatel publicou dado novo. Isso não trava a tabela, porque o `check_update` compara o mês da fonte com o último mês da tabela (`compare_against="coverage"`), que só avança quando a materialização termina (PR #1783).

**Histórico do bug (contexto):** antes, a função retornava `df["data"].max()` do arquivo inteiro, então o `source_max_date` de cada densidade refletia a data de *outra* geografia (UF/Município), não a própria; e o poll era **eager** (gravava o `Update` antes de materializar). Uma falha de materialização carimbava o ponteiro adiantado e travava a tabela — foi o que travou a `densidade_brasil`.

**Carga sem mês novo:** para recarregar uma tabela sem passar pelo `check_update`, dispara-se o `<tabela>_download` direto, com `download_params={"reference_date": "2026-08-01"}`. O `download` não lê a data: ele sempre baixa o semestre mais recente ou o CSV de densidade inteiro. Num deployment de dev (`dev-...`), o `build_and_promote` não promove para prod.

---

## Tabelas e particularidades

### br_anatel_telefonia_movel__microdados

Acessos por município, operadora, tecnologia, modalidade, tipo de pessoa e produto.

**Problema identificado — `acessos` 100% NULL de 2025-01 a 2026-03 (15 meses):**
- Causa: a fonte passou a emitir `acessos` como float (`"1.0"`) em 2025; `safe_cast(acessos as int64)` nulifica.
- **Fix (APLICADO):** o cast no modelo é `safe_cast(safe_cast(acessos as float64) as int64)`. Retrocompatível (`"1"`→1, `"1.0"`→1, `"null"`→NULL). Como a tabela é `materialized="table"` e o staging guarda os `"1.0"` crus, um `dbt run` recupera os 15 meses **sem rebaixar da Anatel**. Validado em dev (`dbt run`+`dbt test` OK); prod via PR.

**CNPJ com 14 dígitos:** a fonte traz o CNPJ com os zeros à esquerda, mas parte da staging guarda o valor sem eles (13 ou 11 dígitos), de cargas em que a limpeza lia a coluna como número. O modelo completa com `lpad(safe_cast(cnpj as string), 14, '0')`. Nenhum valor da staging passa de 14 dígitos, então o `lpad` não corta nada.

**Partição (corrigida):** o `range` da partição por `ano` foi ajustado para `end: 2031` (2026 + 5, convenção BD) — antes estava em 2023, deixando os anos ≥2023 em partição residual (degradava custo/pruning).

### br_anatel_telefonia_movel__densidade_brasil

Densidade nacional (1 linha por mês).

**Problema identificado — tabela travada em 2026-01** (as outras densidades foram até 2026-03):
- **Não** é ausência na fonte — o arquivo tem `"Brasil"` para todos os meses. É o **bug do poll** (ver seção acima): o ponteiro do brasil foi carimbado adiantado e o flow passou a retornar antes de materializar (falhas de junho/2026).
- **Fix permanente (aplicado):** poll por geografia + deferido (ver seção acima), que impede o ponteiro de avançar sem materialização bem-sucedida.

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
