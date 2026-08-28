# br_bndes_operacoes_contratadas

Conjunto de **operações de financiamento contratadas pelo BNDES**, a partir do **Portal de Dados Abertos do BNDES** (CKAN). Slug de backend do conjunto: `operacoes_contratadas`.

Tabelas:

| Tabela | Grão | Pipeline recorrente |
|---|---|---|
| `operacoes_indiretas_automaticas` | operação (forma indireta automática) | ✅ |
| `operacoes_administracao_publica` | operação com ente da Administração Pública Direta | ✅ |
| `operacoes_exportacao_bens` | subcrédito de operação de exportação pós-embarque de bens | ✅ |
| `operacoes_exportacao_servicos` | subcrédito de operação de exportação pós-embarque de serviços de engenharia | ⏸️ (flow sem cron — carga única) |
| `operacoes_nao_automaticas` | subcrédito (forma direta e indireta não automática) | ✅ |

## Estrutura (compartilhada)

- **Crawler (Prefect 3):** `pipelines/crawler/bndes/{constants,utils,tasks,flows}.py`. Cada tabela com pipeline tem sua própria config (`constants` / `constants_administracao_publica`), seu transform + `clean` e seu `_run` em `flows.py`. Funções genéricas (`download_csv`, `get_source_last_modified`) são compartilhadas entre as tabelas.
- **Wrapper `@flow` + schedule por tabela:** `pipelines/datasets/br_bndes_operacoes_contratadas/flows.py` (cron **semanal**, segunda 06h BRT).
- **Poll deferido** (`poll_source_for_update` + `commit_source_update`): grava o Poll ao detectar novidade, mas só comita o Update **depois** de materializar — evita adiantar o Update e travar runs futuras se o flow falhar no meio.
- **Staging 100% STRING:** o `clean` grava Parquet todo string; a tipagem fica a cargo do `safe_cast` no dbt. (Parquet tipado quebra o upload: `... does not match target STRING_PIECE`.) Partição por `ano`.
- **DBT:** `models/br_bndes_operacoes_contratadas/` (um `.sql` por tabela + `schema.yml` único).
- **Observation level = `transaction`** (grão de operação/subcrédito; a BD não tem entidade "operação").

## operacoes_indiretas_automaticas

### O que é

Operações de financiamento **contratadas** pelo BNDES na forma **indireta automática** (menor valor, repassadas por instituições financeiras credenciadas). Grão = **uma operação contratada** (não há identificador único de operação na fonte). Cobertura nacional, **2002-01 a 2026-05**, ~2,36 milhões de linhas. Não inclui Cartão BNDES nem operações com pessoas físicas (o documento do cliente é sempre CNPJ).

### Fonte

CSV consolidado do Portal de Dados Abertos do BNDES (CKAN), recurso `612faa0b-b6be-4b2c-9317-da5dc2c0b901` (`;`-delimitado, cp1252, ~1,1 GB, série inteira em um arquivo). Sinal de atualização = **`last_modified`** do recurso, via `GET /api/3/action/resource_show?id=612faa0b-b6be-4b2c-9317-da5dc2c0b901`.

### Decisões de modelagem

- **Partição só por `ano`** (INT64), derivado de `data_contratacao`.
- **`id_municipio` sentinelas → NA:** `"0"` e `"9999999"` (município não informado) viram NA, pra não criar FK quebrado contra `br_bd_diretorios_brasil.municipio` (nulo passa no teste).
- **CNAE não vira FK** (classificação própria do BNDES; CNAE 2.2 ≠ `cnae_2` do diretório).
- **`has_sensitive_data = no`** (CNPJ mascarado na origem; varredura confirmou zero CPF).
- **Nome da tabela** — paralelo à irmã `operacoes_nao_automaticas`, sem redundância com o conjunto ("Operações Contratadas") e ≤3 palavras (manual de estilo). O nome inicial gerado por IA (`operacoes_contratadas_forma_indireta_automatica`) foi ajustado em review.
- DBT sem `unique_combination` (grão-operação sem PK). Os testes de `relationships` são escopados a `__most_recent_year__` (tabela grande).

### Limitações conhecidas

- **Precisão do poll (data, não datetime).** O `last_modified` do CKAN (com microssegundos) é truncado para **data** no poll (`SOURCE_DATE_FORMAT = "%Y-%m-%d"` em `flows.py`). Se a fonte publicar **duas vezes no mesmo dia**, a 2ª revisão teria a mesma data da 1ª e seria pulada. **Impacto desprezível** com o cron **semanal**: cada run pega a versão mais recente (o CSV é regenerado/cumulativo) e o framework de poll compartilhado coerce para data de qualquer forma — o fix teria que ser no framework, não aqui. Documentado por indicação de code review.
- **Workspace em `/tmp` fixo.** `INPUT_PATH`/`OUTPUT_PATH` são compartilhados; o `download_csv` retoma via `Range` e o `clean` faz `rmtree` do output antes de reescrever. Runs concorrentes se atropelariam — mas o flow é semanal/single-run e isso segue a convenção dos outros crawlers do repo. O `download_csv` valida o tamanho final (falha alto em vez de corromper silenciosamente).
- **Datas inválidas em `data_contratacao`** virariam `ano` nulo e **não** entram no Parquet (não dá pra particionar por nulo). Hoje são **0** (CSV == xlsx verificado ao centavo), e o `clean` **loga e descarta explicitamente** essas linhas quando ocorrem (não é mais silencioso).

### Metadados

Registrados **direto em produção** (o backend de dev foi desativado durante a onboarding): no conjunto existente `operacoes_contratadas`, tabela em status **`under_review`** (aguardando code review para promover a `published`). Descrições PT/EN/ES, coverage **2002-01 a 2026-05** (ano-mês, refletindo a atualização mensal da fonte), cloud table em `basedosdados.br_bndes_operacoes_contratadas`. A raw source (nome = nome da tabela) tem o Update mensal preenchido; o Poll é gravado na 1ª run.

## operacoes_administracao_publica

### O que é

Operações do BNDES com **entes da Administração Pública Direta** (União, Administração Estadual, Administração Municipal). Grão = uma operação. Cobertura nacional, **1994–2026**, ~4,7 mil linhas (após o filtro CONTRATADA). Fonte: conjunto CKAN `operacoes-com-entes-da-administracao-publica-direta`, recurso `ea4e5da3-e586-4225-a460-c5aa09e36100` (~1,17 MB, `;` / cp1252). Sinal de atualização = `last_modified` do recurso, mensal.

### Decisões de modelagem

- **Filtra só `nivel_atual == 'CONTRATADA'`** (95,1% das linhas). A fonte publica o funil inteiro (PERSPECTIVA → C/CONSULTA → EM ANÁLISE → APROVADA → CONTRATADA), mas o conjunto é "operações contratadas". O filtro fica isolado numa linha do `clean` e a coluna `nivel_atual` **permanece no schema** (constante), pra que remover o filtro depois não exija mudança de estrutura.
- **Valores em REAIS, não em milhares — `measurement_unit = BRL`, sem ×1000.** O dicionário de dados oficial do BNDES (PDF) descreve `valor_da_operacao_historico_em_reais`, `valor_desembolsado_em_reais` e `saldo_a_liberar_atualizado_em_reais` como "em milhares de reais", mas isso é **boilerplate errado**: o nome das colunas diz "em reais" e a ordem de grandeza confirma reais — a maior operação bruta é 3.605.000.000 (Plano de Mobilidade de SP), coerente com **R$3,6 bi**; em milhares seria R$3,6 **trilhões** numa única operação (impossível). **Não multiplicar por 1000.** (Decisão Davi + revisão, contra a doc oficial.)
- **Geografia pelo diretório:** `sigla_uf` (de `uf`, `-`→NA) e `id_municipio` resolvido no dbt por join normalizado (nome+UF) contra `br_bd_diretorios_brasil.municipio` — o nome do município **não** é guardado na tabela final (manual de estilo). 98,4% dos municípios casam automaticamente; ~8 variantes de grafia/renomeação entram por CASE; `DIVERSOS`/`SEM MUNICIPIO`/`-` viram NA. O staging mantém `nome_municipio` (all-string) só para essa resolução.
- `saldo_a_liberar` vem tipado como `string` no dicionário, mas é 100% numérico nos dados → `FLOAT64`, BRL.
- `has_sensitive_data = no` (entes públicos, sem CPF/CNPJ de pessoa). Cobertura pública → `AllFree`, sem paywall BD Pro.

## operacoes_exportacao_bens

### O que é

Operações de financiamento à **exportação pós-embarque de bens** (comercialização de bens brasileiros no exterior; os desembolsos são feitos no Brasil, em reais, ao exportador). Grão = **subcrédito**: o dicionário do BNDES diz que cada operação pode ter um ou mais subcréditos, com condições financeiras distintas, e que o somatório das linhas com o mesmo número de operação equivale ao valor total da operação. Cobertura nacional, **2002-01 a 2026-06**, ~2,3 mil linhas.

### Fonte

Conjunto CKAN `operacoes-exportacao`, recurso `0cfe4594-44bf-48a8-a79a-686fc2d0db95` (~978 KB, `;` / cp1252). O mesmo conjunto publica pré-embarque e pós-embarque de serviços de engenharia — recursos distintos, fora desta tabela. Sinal de atualização = `last_modified` do recurso, mensal.

### Decisões de modelagem

- **A fonte não publica valores.** O BNDES omite os montantes por sigilo de preço unitário dos bens, então a tabela não tem nenhuma coluna monetária — é um catálogo de operações. Por isso `parse_decimal_ptbr` não é usado aqui.
- **Sem chave primária, e sem `unique_combination_of_columns`** — igual às irmãs. Nenhuma combinação de colunas identifica a linha: 47 linhas são idênticas a outra em todas as 21 colunas da fonte. `id_operacao` tem 1.888 valores distintos em 2.321 linhas; o prefixo `id_` marca a entidade, não unicidade. A repetição é estrutural (subcréditos da mesma operação, e mais de um contrato/desconto de título por operação — o que também explica datas diferentes para o mesmo número).
- **`id_operacao` tem dois formatos.** 796 linhas trazem só o número de 7 dígitos (117 com zero à esquerda significativo) e 1.525 vêm como `numero_base/desdobramento` (`2272455/0001`). Os desdobramentos são exclusivos do produto `BNDES EXIM PÓS-EMBARQUE - EXIM AUTOMÁTICO` — a linha em que o BNDES desconta carta de crédito emitida por banco estrangeiro credenciado, que analisa o crédito do importador e assume o risco comercial, em vez de o próprio BNDES analisar cada operação. A correspondência com `produto` é exata. O dicionário os atribui a "cada número base poder ter um exportador/importador diferente", mas isso só se confirma em 25 das 210 bases com mais de um desdobramento (o importador não é publicado). Fica STRING, sem desmembrar.
- **`setor_subsetor_de_atividade` vira `setor_bndes` + `subsetor_bndes`, casando o setor por prefixo** (detalhe e validação na subseção abaixo). É agrupamento estatístico próprio do BNDES ("agrupamentos de códigos das seções e divisões da CNAE"), então **não vira FK de CNAE** — mesma decisão da `operacoes_indiretas_automaticas`.
- **`tipo_garantia` é multivalorado e precisa de normalização.** A operação pode combinar vários tipos, separados por `/`, e a fonte varia o espaçamento e a caixa (`Real / Pessoal` e `Real/ Pessoal`; `Seguro de crédito/FGE`, `Seguro de Crédito / FGE` e `Seguro de crédito/ FGE`). O `clean` padroniza o separador de combinação para ` / `, protegendo antes os rótulos que têm barra no próprio nome (`Seguro de crédito/FGE`, `CCR/ALADI`) — 15 grafias viram 11 valores. É a primeira normalização de grafia do conjunto.
- **Geografia pelo diretório:** `sigla_uf` direto da fonte; `pais_destino` (NOME) fica como `nome_pais_destino` no staging e vira **`sigla_pais_destino`** (ISO 3166-1 alfa-3) no dbt, por join normalizado (maiúsculas, sem acento) contra `br_bd_diretorios_mundo.pais` — mesmo desenho que a `operacoes_administracao_publica` usa para município. A coluna é `sigla_`, e não `id_`, porque a chave do diretório é `sigla_iso3`; não existe `id_pais`. `DIVERSOS` (82 linhas) vira NA; dos 25 países, 24 casam automaticamente e só `PAISES BAIXOS(HOLAN)` entra por CASE (no diretório o nome é "Holanda", `NLD`).
- **`sigla_moeda` normalizada para ISO**: a fonte traz `US$ COMPRA` e `EUR C`.
- **`descricao_da_operacao` → `tipo_operacao`** e **`mutuario` → `tipo_mutuario`**: são categóricos de 2 valores e `descricao_` não está entre os prefixos do manual de estilo. Atenção na descrição da coluna: o **mutuário é o ente estrangeiro** responsável pelo pagamento, não o exportador.
- **Tipos do dicionário oficial não são confiáveis** (mesmo padrão do erro de unidade em `operacoes_administracao_publica`): ele declara `CNPJ do Exportador` como `int64` — o CSV traz `88.611.835/0001-29`, com pontuação — e `Numero da operacao` como `Int64`, que contém `/` e zeros à esquerda. Ambos são STRING.
- Quatro colunas são constantes na série inteira (`area_operacional`, `modalidade_apoio`, `forma_apoio`, `categoria`) e permanecem no schema. O dicionário explica: toda operação de financiamento à exportação do BNDES é reembolsável, e toda a base é do produto BNDES Exim Pós-embarque (o Exim Automático é a indicação adicional em `produto`).
- Cobertura pública → `AllFree`, sem paywall BD Pro.

### Corte de `setor_subsetor_de_atividade`

Nenhum separador serve de corte fixo: `COMERCIO/SERVICOS` tem barra no próprio nome e aparece tanto sozinho quanto seguido de subsetor. O `clean` casa o início do valor contra os setores conhecidos (`constants_exportacao_bens.SETORES`, do rótulo mais longo para o mais curto) e trata o resto como subsetor; valor que não comece por um setor conhecido levanta exceção, porque significa que a fonte mudou os rótulos.

Amostra do arquivo de 2026-06, uma linha de cada forma do campo (2.202, 106, 4 e 9 linhas, nessa ordem).

**Antes do tratamento** — o que a fonte publica:

| `numero_da_operacao` | `data_da_contratacao` | `exportador` | `setor_subsetor_de_atividade` |
| --- | --- | --- | --- |
| 0704109 | 2002-01-08 | SCHULZ S/A | `INDUSTRIA/METALURGIA` |
| 1972983 | 2009-09-04 | A L HECHER MADEIRAS LTDA | `COMERCIO/SERVICOS/COMERCIO VAREJISTA` |
| 1712455 | 2008-01-30 | A.R.G. S.A. | `COMERCIO/SERVICOS` |
| 0893599 | 2002-04-17 | TRAMONTINA FARROUPILHA SA INDUSTRIA METALURGICA | `INDUSTRIA` |

**Depois do tratamento** — as duas colunas da tabela final, com o que o corte no último `/` daria:

| `id_operacao` | `setor_bndes` | `subsetor_bndes` | Corte no último `/` |
| --- | --- | --- | --- |
| 0704109 | `INDUSTRIA` | `METALURGIA` | igual |
| 1972983 | `COMERCIO/SERVICOS` | `COMERCIO VAREJISTA` | igual |
| 1712455 | `COMERCIO/SERVICOS` | nulo | `COMERCIO` + `SERVICOS` ❌ |
| 0893599 | `INDUSTRIA` | nulo | igual |

O corte no último `/` acerta as outras três formas; só erra na terceira, em que o setor composto vem sozinho — ali ele cria um setor `COMERCIO` e um subsetor `SERVICOS` que não existem na fonte, e a tabela fecha com 3 setores e 28 subsetores em vez dos 2 e 27 reais.

**Validação** (2.321 linhas, arquivo de 2026-06): recompondo `setor_bndes` + `/` + `subsetor_bndes` chega-se ao valor original em todas as 2.321 linhas, ou seja, o corte não descarta nem inventa caractere. Nenhuma linha fica sem setor e 13 ficam sem subsetor (as 9 `INDUSTRIA` e as 4 `COMERCIO/SERVICOS` publicadas sem subsetor). Resultado: 2 setores (`INDUSTRIA` 2.211, `COMERCIO/SERVICOS` 110) e 27 subsetores.

### Notas para descrição de coluna

- `porte_exportador` é o porte **na data da contratação**, pela política vigente à época — não é comparável ao longo da série.
- `fonte_recurso` refere-se aos **desembolsos**; um contrato pode ter várias fontes entre seus subcréditos.
- `custo_financeiro` pode ser composto (variação cambial + indexador); em `Taxa de juros em moeda estrangeira` a taxa é só variação cambial + juros.
- `modalidade_operacional`: `Supplier` = desconto de títulos de crédito; `Buyer` = apenas contrato de financiamento.

## operacoes_exportacao_servicos

### O que é

Operações de financiamento à **exportação pós-embarque de serviços de engenharia**, destinadas a obras executadas no exterior por empresas brasileiras. Grão = **subcrédito**, mesma definição da irmã de bens: 146 operações em 652 linhas, e o somatório das linhas com o mesmo `id_operacao` equivale ao valor total da operação. Cobertura nacional, **1998-07-24 a 2015-04-28**.

### Fonte

Mesmo conjunto CKAN `operacoes-exportacao` da irmã de bens, recurso `d158033b-f6cb-4609-9717-a9cb2ff7ffc5`, com dicionário de dados próprio (recurso `184b2403-71ad-47ca-9e12-e55970bafc5d`). Sinal de atualização = `last_modified` do recurso.

**Baixa pelo `/datastore/dump`, não pelo `result.url` do `resource_show`.** Os dois caminhos servem o mesmo conteúdo (conferido célula a célula); o dump vem UTF-8, com decimal em ponto e data ISO, enquanto o download direto vem cp1252 com decimal em vírgula. O dump dispensa a conversão decimal — por isso `parse_decimal_ptbr` não é usado aqui, apesar de esta ser a primeira tabela do conjunto com valores.

### Carga única, sem cron

**A série termina em 2015 e não recebe operação nova há dez anos.** O recurso segue sendo republicado no CKAN (`last_modified` de 2025-07-09), mas isso é republicação do arquivo, não cobertura nova. O `@flow` existe e está completo, com poll e tudo o mais, mas **sem `deploy_schedules`** — roda por disparo manual. Se a fonte voltar a publicar, acrescentar o `deploy_schedules` basta. O precedente no repositório é o `br_ms_sinan`, o único outro flow sem cron.

### Decisões de modelagem

As 21 colunas comuns às duas tabelas de exportação seguem as decisões já tomadas na irmã de bens (`setor_subsetor` desmembrado casando o setor por prefixo, `id_operacao` STRING, geografia pelo diretório, sem `unique_combination_of_columns`). A lista de setores conhecidos é própria de cada tabela: aqui o agrupamento se escreve `COMERCIO E SERVICOS`, contra `COMERCIO/SERVICOS` em bens. O que difere:

- **A fonte publica valores aqui, e em dólar.** `valor_operacao`, `valor_desembolsado`, `taxa_juros` e `prazo_meses` vêm preenchidos em 100% das linhas — são as quatro colunas que bens não tem. `moeda_sigla` é `US$ COMPRA` em toda a série, então `measurement_unit = USD` nas duas colunas monetárias (em bens o campo tem dois valores).
- **`descricao_da_operacao` é texto livre, não categórico.** Em bens a coluna tem 2 valores e virou `tipo_operacao`; aqui tem 146 — uma descrição de obra por operação —, então vira `descricao_operacao`. As duas tabelas divergem no nome porque divergem no conteúdo.
- **`tipo_garantia` normaliza em CAIXA ALTA.** Mesma mecânica da irmã (`_normalize_garantia`), mas os rótulos compostos são publicados aqui em maiúscula (`SEGURO DE CRÉDITO/FGE`, `CCR/ALADI`), então cada tabela passa os seus. 6 grafias viram 5 valores.
- **Cinco linhas trazem o byte `0x90` no meio de uma palavra**, em quatro grafias que corrompem a mesma palavra de formas diferentes (`HIDREL\x90ETRICA`, `HIDREL\x90TRICA`, `PERIF\x90ÉRICA`, `PERIFE\x90RICA`) — ora no lugar da letra acentuada, ora ao lado dela. Não há regra genérica que acerte as quatro, então a correção é um mapa explícito em `DESCRICAO_CORRECOES` e o `clean` **falha alto** se sobrar `0x90` em alguma linha.
- **Contagem de linhas como contraprova do download.** O `/datastore/dump` responde `Transfer-Encoding: chunked`, sem `Content-Length`, e ignora `Range` — o `download_csv` roda com `validate_size=False` e não consegue conferir bytes. No lugar disso, o `clean` compara as linhas lidas com o `total` que a API do datastore declara (`assert_row_count`).
- **Nove colunas são constantes na série** (contra quatro em bens), incluindo `setor_subsetor_de_atividade`, `fonte_recurso` e `custo_financeiro`. Todas sobem: os valores são verdadeiros, a constância é desta fatia e não do campo, e a simetria com a irmã mantém o `union` direto.
- **Nenhum país fica sem correspondência**: 15 países, todos casando contra `br_bd_diretorios_mundo.pais` pela normalização já usada em bens. Não existe `DIVERSOS` aqui, e o `CASE` de `PAISES BAIXOS(HOLAN)` não é necessário.
- Cobertura pública → `AllFree`, sem paywall BD Pro.

### Notas para descrição de coluna

- `cnpj_exportador` e `nome_exportador` **não são 1:1** (19 CNPJs, 20 nomes): consórcios são registrados sob o CNPJ da empresa líder, com as consorciadas no nome.
- `taxa_juros` varia entre os subcréditos de uma mesma operação conforme a data de embarque ou de prestação em cada liberação — média simples entre linhas não faz sentido sem ponderar pelo valor.
- `custo_financeiro` é `TAXA FIXA EM US$` em toda a série, então `taxa_juros` é a taxa total.
- `tipo_mutuario` é quase sempre público aqui (642 contra 10), inverso de bens.

## operacoes_nao_automaticas

Operações contratadas na forma **direta e indireta não automática**; cada contrato pode ter um ou mais subcréditos e **cada linha é um subcrédito** (grão = subcrédito). Onboardada antes desta sprint; o crawler a atende pelo config genérico `constants.TABLES_CONFIGS`, com o mesmo `_run_operacoes` da `operacoes_indiretas_automaticas`. Decisões de modelagem não documentadas neste README.
