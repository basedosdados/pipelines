# br_senado_dados_abertos_administrativos

Dados administrativos do Senado Federal: despesas da cota parlamentar (CEAPS), senadores,
servidores, folha de pagamento, horas extras, contratações, licitações, colaboradores, suprimento
de fundos e quadros de pessoal. As 40 tabelas vêm da API de Dados Abertos Administrativos do
Senado; a dimensão `senador` usa também a API de Dados Abertos Legislativos. No backend, o slug do
conjunto é `dados_abertos_administrativos`.

| Tabela | Grupo | Partição | Coluna de data | Atualização |
|---|---|---|---|---|
| `senador` | Senadores | — | — | diária |
| `despesa_ceaps` | Senadores | `ano` | `data` | diária |
| `senador_gabinete` | Senadores | `data_extracao` | `data_extracao` | diária |
| `senador_escritorio_apoio` | Senadores | `data_extracao` | `data_extracao` | diária |
| `senador_auxilio_moradia` | Senadores | `data_extracao` | `data_extracao` | diária |
| `senador_aposentado_pensionista` | Senadores | `data_extracao` | `data_extracao` | diária |
| `servidor` | Servidores | `data_extracao` | `data_extracao` | diária |
| `servidor_ativo` | Servidores | `data_extracao` | `data_extracao` | diária |
| `servidor_remuneracao` | Servidores | `ano` | `ano`, `mes` | diária |
| `servidor_hora_extra` | Servidores | `ano` | `ano`, `mes` | diária |
| `servidor_hora_extra_dia` | Servidores | `ano` | `data` | diária |
| `servidor_aposentado` | Servidores | `data_extracao` | `data_extracao` | diária |
| `servidor_exonerado` | Servidores | `data_extracao` | `data_extracao` | diária |
| `servidor_cedido` | Servidores | `data_extracao` | `data_extracao` | diária |
| `pensionista` | Servidores | `data_extracao` | `data_extracao` | diária |
| `contratacao` | Contratações | `data_extracao` | `data_extracao` | diária |
| `contratacao_orgao_gestor` | Contratações | `data_extracao` | `data_extracao` | diária |
| `contratacao_item` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `contratacao_garantia` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `contratacao_pagamento` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `contratacao_documento_fiscal` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `contratacao_pagamento_empenho` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `contrato_aditivo` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `ata_acionamento` | Contratações | `data_extracao` | `data_extracao` | semanal |
| `licitacao` | Contratações | `data_extracao` | `data_extracao` | diária |
| `licitacao_detalhamento` | Contratações | `data_extracao` | `data_extracao` | diária |
| `empresa` | Contratações | `data_extracao` | `data_extracao` | diária |
| `terceirizado` | Colaboradores | `data_extracao` | `data_extracao` | diária |
| `menor_aprendiz` | Colaboradores | `data_extracao` | `data_extracao` | diária |
| `estagiario` | Colaboradores | `data_extracao` | `data_extracao` | diária |
| `suprido_ato_concessao` | Supridos | `ano` | `data` | diária |
| `suprido_empenho` | Supridos | `ano` | `data` | diária |
| `suprido_transacao` | Supridos | `ano` | `data` | diária |
| `suprido_transacao_objeto` | Supridos | `ano` | `ano` | diária |
| `suprido_movimentacao` | Supridos | `ano` | `data` | sem horário |
| `suprido_movimentacao_subtipo` | Supridos | `ano` | `ano` | sem horário |
| `quadro_pessoal` | Gestão | `data_extracao` | `data_extracao` | diária |
| `diretor_coordenador` | Gestão | `data_extracao` | `data_extracao` | diária |
| `previsao_aposentadoria` | Gestão | `data_extracao` | `data_extracao` | diária |
| `dicionario` | Dicionário | — | — | só na carga completa |

A coluna de data é a que define a cobertura registrada no backend e a faixa do BD Pro.

As 10 tabelas particionadas por `ano` são séries históricas. As 28 particionadas por
`data_extracao` são snapshots: para elas, a API só expõe o estado atual, e cada run grava um
snapshot novo, com a data em que rodou. `senador` e `dicionario` não têm partição.

## Fonte

API de Dados Abertos Administrativos do Senado Federal, em
https://adm.senado.gov.br/adm-dadosabertos. É pública e não pede autenticação nem chave. As
chamadas usam a base
`https://adm.senado.gov.br/adm-dadosabertos/api/v1`, com o cabeçalho `Accept: application/json` e
um User-Agent de navegador.

A API administrativa não tem código de senador, só nomes. O `id_senador` vem da API de Dados
Abertos Legislativos, em https://legis.senado.leg.br/dadosabertos, a mesma do conjunto irmão
`br_senado_dados_abertos` (ver a seção de `senador`).

Endpoints por tabela, com caminhos relativos à base da API administrativa. Nas contratações,
`{tipo}` é `contratos`, `notas_empenho` ou `atas_registro_preco`:

| Tabela | Endpoint |
|---|---|
| `senador` | API legislativa: `/senador/lista/legislatura/{legislatura}`, uma chamada por legislatura; API administrativa: `/senadores` |
| `despesa_ceaps` | `/senadores/despesas_ceaps/{ano}` |
| `senador_gabinete` | `/senadores` |
| `senador_escritorio_apoio` | `/senadores/escritorios-apoio` |
| `senador_auxilio_moradia` | `/senadores/auxilio-moradia-imoveis-funcionais` |
| `senador_aposentado_pensionista` | `/senadores/aposentados`, `/senadores/aposentados-ipc`, `/senadores/aposentados-pssc`, `/senadores/pensionistas-ipc` |
| `servidor` | `/servidores/servidores` |
| `servidor_ativo` | `/servidores` |
| `servidor_remuneracao` | `/servidores/remuneracoes/{ano}/{mes}`; uma chamada por mês |
| `servidor_hora_extra`, `servidor_hora_extra_dia` | `/servidores/horas-extras/{ano}/{mes}`; uma chamada por mês |
| `servidor_aposentado` | `/servidores/aposentados-efetivos`, `/servidores/aposentados-comissionados` |
| `servidor_exonerado` | `/servidores/exonerados` |
| `servidor_cedido` | `/servidores/cedidos/para-senado`, `/servidores/cedidos/pelo-senado`, `/servidores/cedidos/infraero-para-senado`, `/servidores/exercicio-provisorio` |
| `pensionista` | `/servidores/pensionistas` |
| `contratacao`, `contratacao_orgao_gestor` | `/contratacoes/contratos?statusContratoParam=<status>`, uma chamada por status; `/contratacoes/notas_empenho`; `/contratacoes/atas_registro_preco` |
| `contratacao_item` | `/contratacoes/{tipo}/{id}/itens`; uma chamada por contratação |
| `contratacao_garantia` | `/contratacoes/{tipo}/{id}/garantias`; uma chamada por contratação |
| `contratacao_pagamento`, `contratacao_documento_fiscal` | `/contratacoes/{tipo}/{id}/pagamentos`; uma chamada por contratação |
| `contratacao_pagamento_empenho` | `/contratacoes/{tipo}/{id}/pagamentos/{id_pagamento}/empenhos`; uma chamada por pagamento |
| `contrato_aditivo` | `/contratacoes/contratos/{id}/aditivos`; uma chamada por contrato |
| `ata_acionamento` | `/contratacoes/atas_registro_preco/{id}/acionamentos`; uma chamada por ata |
| `licitacao`, `licitacao_detalhamento` | `/contratacoes/licitacoes` |
| `empresa` | `/contratacoes/empresas` |
| `terceirizado` | `/contratacoes/terceirizados` |
| `menor_aprendiz` | `/contratacoes/menores_aprendizes` |
| `estagiario` | `/colaboradores/estagiarios` |
| os seis `suprido_*` | `/supridos/{ano}` |
| `quadro_pessoal` | seis endpoints de quantitativo (ver a seção da tabela) |
| `diretor_coordenador` | `/gestao/diretores-e-coordenadores` |
| `previsao_aposentadoria` | `/servidores/previsao-aposentadoria` |
| `dicionario` | — (montado no tratamento) |

### Endpoints não lidos

| Endpoint | Motivo |
|---|---|
| os caminhos `/csv` | mesmo conteúdo do endpoint JSON correspondente |
| `/servidores/servidores/{ativos,inativos,efetivos,comissionados}` | subconjuntos de `/servidores/servidores`, sem nenhuma linha fora dele (ver `servidor`) |
| `/servidores/pensionistas/remuneracoes/{ano}/{mes}` | devolve o mesmo conteúdo, byte a byte, de `/servidores/remuneracoes/{ano}/{mes}` |
| `/supridos/{atosConcessao,empenhos,transacoes,movimentacoes}/{ano}` | o mesmo conteúdo que `/supridos/{ano}` traz aninhado |
| `/contratacoes/licitacoes/{id}/detalhamentos` e `/{id_detalhamento}` | os detalhamentos já vêm aninhados em `/contratacoes/licitacoes` |
| `/contratacoes/{tipo}/{id}/pagamentos/{id_pagamento}/documentos_fiscais` | devolve 0 linhas mesmo quando a lista aninhada no pagamento tem documentos |
| `/gestao/*/agrupado`, `/senadores/{cod}/recursos-utilizados` | agregações e cruzamentos de endpoints já lidos |
| `/servidores/aposentados-{efetivos,comissionados}/{matricula}` | consulta de um registro dos endpoints de lista |
| `/contratacoes/empresas?pagina=` | trunca a lista (ver `empresa`) |
| campos `contratos`, `notas_empenho` e `atas_registro_preco` de `/contratacoes/empresas` | os três devolvem a mesma lista, com menos campos (ver `empresa`) |

## Particularidades da fonte

### 404 é ausência de registro

A API responde 404 quando não há registro: um ano anterior ao início da série, uma contratação sem
itens. `get_json` devolve lista vazia para 404, sem tentar de novo.

### Novas tentativas

`get_json` tenta até 5 vezes, esperando 1,5 s × o número da tentativa entre uma e outra. Um 200
com corpo vazio entra nas novas tentativas. Se todas falham, a chamada levanta erro: status
diferente de 200 e de 404, corpo vazio ou erro de rede nunca chegam ao tratamento como lista vazia.

`get_legislativo_json`, que lê a API legislativa, segue o mesmo esquema, mas trata 404 como erro.

### Dois envelopes

Os endpoints devolvem uma lista JSON solta ou um envelope `{statusCode, msg, data}`, com a lista
em `data`. `fetch_records` lê os dois formatos e devolve sempre uma lista de registros.

### Chamadas por entidade

A API leva cerca de 1,8 s por requisição e atende cerca de 6 requisições por segundo com 10
conexões simultâneas. Com mais de 10 conexões simultâneas, as requisições começam a falhar.
`fan_out` faz as chamadas por entidade (por contratação, por pagamento) 8 de cada vez.

Quando a chamada de uma entidade falha em todas as tentativas, `fan_out` devolve `None` para ela,
e a entidade fica sem filhas naquele run. Até 1% de falhas no lote, o run segue e o log registra a
contagem e as primeiras entidades que falharam. Acima de 1%, `fan_out` levanta erro e o run para.

### Formatos misturados

A mesma API usa formatos diferentes de um endpoint para outro. O tratamento converte todos para um
só:

| Tipo | Como chega | Como sai |
|---|---|---|
| Data | `AAAA-MM-DD` em contratações e supridos; `dd/mm/aaaa` em servidores e senadores | `AAAA-MM-DD`; outro formato vira nulo |
| Data-hora | `AAAA-MM-DD HH:MM:SS`, com espaço ou `T` | `AAAA-MM-DD HH:MM:SS` |
| Valor | número JSON ou texto no formato brasileiro (`16.368,74`) | número com ponto decimal |
| Booleano | `true`/`false` ou `S`/`N` | `sim`/`nao` |
| Horas | `02h00` | horas decimais (`01h30` → `1.5`) |
| Jornada semanal | `40 Horas` | inteiro |

Vazio, `-`, `---`, `N/A`, `NAO INFORMADO` e `NÃO INFORMADO` viram nulo.

## Formato dos dados

Todas as colunas saem como texto no parquet: a staging é toda STRING, e o modelo dbt faz o
`safe_cast` para o tipo final. Nulo sai como nulo, nunca como o texto `"nan"`.

`write_table` grava:

- as séries em `<tabela>/ano=<n>/data.parquet`;
- os snapshots em `<tabela>/data_extracao=<AAAA-MM-DD>/data.parquet`;
- `senador` e `dicionario` num único `<tabela>/data.parquet`.

A coluna de partição fica só no nome da pasta, não no arquivo. Linha sem valor na coluna de
partição é descartada. Tabela particionada sem nenhuma linha não gera arquivo.

## Arquitetura

A ordem das colunas de cada tabela está no `COLUMNS` da classe `constants_<tabela>` em
`constants.py`; o dicionário `TABLES_CONSTANTS` junta as 40 classes. Os modelos dbt e o
`schema.yml`, com tipos, descrições e testes, ficam em
`models/br_senado_dados_abertos_administrativos/` e são editados à mão.

Os 38 modelos particionados são `incremental`, com `incremental_strategy="insert_overwrite"` na
coluna de partição: um `dbt run` substitui só as partições que estão na staging. `senador` e
`dicionario` são `table` e são refeitos inteiros.

Os nomes das tabelas seguem os de `br_camara_dados_abertos` quando o conceito é o mesmo
(`despesa`, `licitacao`, `servidor`).

## Atualização

Prefect 3, no modelo em estágios: `check_update` → `extract_and_load` → `build_and_promote`. Cada
uma das 39 tabelas com flow tem seu par `check_update`/`extract_and_load` em `flows.py`, montado
por `make_pipeline`, de `tasks.py`. As funções de leitura da API e uma função de limpeza por
tabela (`clean_<tabela>`) ficam em `utils.py`; a task `extract_table`, em `tasks.py`, escolhe a
função de cada tabela. O `dicionario` não tem flow.

### Tabelas que usam as mesmas chamadas

Cada tabela refaz as chamadas de que precisa, mesmo quando outra tabela usa as mesmas:

| Tabelas | Função que cada uma chama de novo |
|---|---|
| `senador`, `senador_gabinete`, `senador_escritorio_apoio`, `senador_auxilio_moradia` | `get_senador_reference` |
| `servidor_hora_extra`, `servidor_hora_extra_dia` | `clean_servidor_hora_extra` |
| as nove tabelas de contratação | `fetch_contratacoes` |
| `contratacao_pagamento`, `contratacao_documento_fiscal`, `contratacao_pagamento_empenho` | `clean_contratacao_pagamento` |
| `licitacao`, `licitacao_detalhamento` | `clean_licitacao` |
| os seis `suprido_*` | `clean_supridos` |

### Data de referência

| Tabelas | Data de referência do `check_update` | Comparada com |
|---|---|---|
| `senador` | a data do dia | o `Table.Update` (`compare_against="table_update"`) |
| as 28 de `data_extracao` | a data do dia | a cobertura |
| as séries com `data` ou com `ano` e `mes` | a maior data da janela; numa série por mês, o primeiro dia do maior mês | a cobertura |
| `suprido_transacao_objeto`, `suprido_movimentacao_subtipo` | a maior `data` da série irmã, em `REFERENCE_DATE_TABLES` | a cobertura |

`senador` e as tabelas de `data_extracao` não têm data de publicação, e a carga é disparada em
todo run. Nas tabelas de `data_extracao`, um segundo run no mesmo dia não recarrega, porque a
cobertura já termina na data do dia.

Nas séries, o `check_update` extrai a janela só para ler a maior data; quando há novidade, o
`extract_and_load` extrai a janela de novo. Quando a janela vem vazia, a data de referência é
1900-01-01, lida como "sem novidade".

`suprido_transacao_objeto` e `suprido_movimentacao_subtipo` só têm `ano`, e uma cobertura por ano
termina em 1º de janeiro: a maior data da própria série nunca passaria dela antes da virada do
ano. Por isso usam a `data` de `suprido_transacao` e de `suprido_movimentacao`, que vêm da mesma
chamada. Na prática, recarregam em todo run assim que o ano corrente tem algum registro.

### Janela de cada run

- Séries (`ano`): o ano corrente e o anterior, para pegar registros do ano anterior publicados
  depois da virada do ano.
- Snapshots (`data_extracao`): o estado atual da API, com `data_extracao` igual à data do run.
- `senador`: a dimensão inteira.

### Carga

O `extract_and_load` sobe a tabela para a staging de `basedosdados-dev` com
`dump_mode="overwrite"`, e a staging fica só com a janela do run. O `build_and_promote` roda o
`dbt run` e o `dbt test` em dev e promove para prod só as partições gravadas no run. O modelo dbt
substitui na tabela final só as partições da janela e deixa as outras como estão: o histórico se
acumula na tabela final, não na staging. Rodar de novo no mesmo dia substitui as mesmas partições,
sem duplicar.

Uma tabela de `data_extracao` que volta da API sem linhas faz o `extract_and_load` falhar em
`save_table`, em vez de subir uma staging vazia.

Quatro tabelas testam `relationships` com `senador`: `despesa_ceaps`, `senador_gabinete`,
`senador_escritorio_apoio` e `senador_auxilio_moradia`. O `check_update` de `senador` roda 21
minutos antes do primeiro delas, para que o teste leia a dimensão já refeita.

O `dicionario` fica de fora: ele é montado com as rubricas dos supridos, e com a janela de dois
anos sairia menor que o da carga completa.

### Metadados

O `check_update` grava o `Poll` da fonte de cada tabela e, quando há novidade, o `Update` da fonte
com a data de referência, antes de disparar o `extract_and_load`. Cada tabela precisa de
exatamente uma fonte original ligada no backend.

O `build_and_promote` atualiza a cobertura da tabela. Em `senador`, que é `NonHistorical`, atualiza
só o `Table.Update`.

### BD Pro

36 tabelas restringem ao BD Pro os 6 meses mais recentes (`PartBdpro`, no `COVERAGE` da classe de
cada tabela em `constants.py`). A cada carga, o `build_and_promote` recalcula a faixa e reaplica as
row access policies do BigQuery.

| Tabelas | Coluna de data | Granularidade | Faixa |
|---|---|---|---|
| as 28 de `data_extracao` | `data_extracao` | dia | `PartBdpro`, 6 meses |
| `despesa_ceaps`, `servidor_hora_extra_dia`, `suprido_ato_concessao`, `suprido_empenho`, `suprido_transacao`, `suprido_movimentacao` | `data` | dia | `PartBdpro`, 6 meses |
| `servidor_remuneracao`, `servidor_hora_extra` | `ano`, `mes` | mês | `PartBdpro`, 6 meses |
| `suprido_transacao_objeto`, `suprido_movimentacao_subtipo` | `ano` | ano | `AllFree` |
| `senador` | — | — | `NonHistorical` |
| `dicionario` | — | — | sem cobertura |

Nas tabelas de `data_extracao`, a parte livre vai até 6 meses antes do snapshot mais recente.
`suprido_transacao_objeto` e `suprido_movimentacao_subtipo` só têm `ano` como data, e uma faixa de
6 meses não se expressa em ano.

### Agendamento

Os horários, em Brasília (`America/Sao_Paulo`), estão no `deploy_schedules` de cada
`check_update`, em `flows.py`:

| Tabelas | Dias | Horário |
|---|---|---|
| `senador` | todo dia | 10:27 |
| as outras 29 tabelas diárias da tabela do início | todo dia | de 5 em 5 minutos, das 10:48 às 13:13 |
| `contratacao_item` | segunda-feira | 13:18 |
| `contratacao_garantia` | terça-feira | 13:18 |
| `contratacao_pagamento` | quarta-feira | 13:18 |
| `contratacao_documento_fiscal` | quinta-feira | 13:18 |
| `contratacao_pagamento_empenho` | sexta-feira | 13:18 |
| `contrato_aditivo` | sábado | 13:18 |
| `ata_acionamento` | domingo | 13:18 |
| `suprido_movimentacao`, `suprido_movimentacao_subtipo` | — | sem horário |

As filhas das contratações são semanais porque fazem uma chamada por contratação, 8 de cada vez
(ver "Chamadas por entidade"). Cada uma roda num dia da semana: duas ao mesmo tempo passariam das
10 conexões simultâneas que a API aguenta. `suprido_movimentacao` e `suprido_movimentacao_subtipo` não têm registro desde
2020; o `check_update` delas só roda quando disparado à mão.

### Memória

Na carga completa, `clean_all` grava e libera uma tabela por vez, e as séries um ano por vez:
`write_table` acrescenta a partição do ano sem apagar as anteriores. Como dicionários Python,
`servidor_remuneracao` ocupa cerca de 2,5 KB por linha, o que dá cerca de 5,9 GB no histórico
completo e cerca de 9 GB somada a `servidor_hora_extra_dia`. Montar todas as tabelas antes de
gravar não cabe num worker de 4 GiB.

Nos flows, `extract_table` monta a janela inteira de uma tabela antes de gravar. A maior é
`servidor_remuneracao`: dois anos de folha, entre 1 e 2 GB pela mesma conta. O `check_update` das
séries também monta a janela, para ler a maior data.

### Carga completa

A carga do histórico completo fica fora do Prefect, em `run_onboarding.py`, neste diretório. O
script chama `clean_all`, grava o parquet em `tmp/br_senado_dados_abertos_administrativos/output/`,
na raiz do repo, e, com `--upload`, sobe cada tabela para a staging de `basedosdados-dev` com
`upload_to_gcs`, em modo `overwrite`. Depois, o `dbt run` constrói os modelos em dev:

```bash
uv run pipelines/datasets/br_senado_dados_abertos_administrativos/run_onboarding.py --full --upload
uv run dbt run --select br_senado_dados_abertos_administrativos
```

Sem `--full`, a extração é uma amostra (séries só do ano corrente, sem as filhas das contratações),
e o upload é recusado. `--skip-extract` sobe o parquet já gravado, sem extrair de novo; `--tables
<tabela> ...` escolhe as tabelas a subir.

A carga completa é a única que refaz o `dicionario` com as rubricas de todos os anos. Nas tabelas de
`data_extracao`, ela grava só o snapshot do dia: o histórico dessas tabelas é a pilha de snapshots
dos runs.

A carga completa faz uma requisição por mês de folha e de hora extra desde 2013 e todas as chamadas
das filhas das contratações, cerca de 61 mil, a cerca de 6 requisições por segundo.

O primeiro período de cada série. Antes dele, a API responde 404:

| Tabelas | Primeiro período |
|---|---|
| `despesa_ceaps` | 2008 |
| `servidor_remuneracao`, `servidor_hora_extra`, `servidor_hora_extra_dia` | janeiro de 2013 |
| os seis `suprido_*` | 2013 |

## Tratamento por tabela

### senador

A API administrativa não tem código de senador, só o nome. `get_senador_reference` lê, na API
legislativa, a lista de parlamentares das legislaturas 53 a 57 (de 2007 a 2027) e monta duas
coisas: o núcleo da dimensão (código, nome parlamentar, nome completo e sexo de cada senador) e o
cruzamento de nome para `id_senador`.

`id_senador` é o `CodigoParlamentar` da API legislativa. Nos 77 casos comparados, ele é igual ao
`codSenador` que a CEAPS traz. Na carga do histórico, a dimensão tinha 681 senadores e cobria os
328 `id_senador` distintos de `despesa_ceaps`.

O cruzamento compara nomes sem acento, em maiúsculas e com espaços simples. Quando o mesmo nome
aparece em mais de uma legislatura, fica o código da mais recente. Um nome tem grafia diferente
nas duas APIs e é mapeado à parte: `NELSINHO TRAD FILHO`, na administrativa, é `Nelsinho Trad` na
legislativa.

Os senadores em exercício são completados com `/senadores`, da API administrativa: UF, partido,
titular ou suplente, mandato, data de nascimento e e-mail, com `indicador_em_exercicio = sim`. Os
demais ficam com `nao` e esses campos nulos. Senador de `/senadores` cujo nome não está no
cruzamento não entra na tabela.

A lista de legislaturas é fixa. Um senador que só aparece a partir da legislatura 58 fica fora da
dimensão e do cruzamento.

### despesa_ceaps

`id_senador` é o `codSenador` da fonte. A tabela não tem o nome do senador: o nome está em
`senador`. `ano` vem do registro ou, quando falta, do ano consultado.

### senador_gabinete, senador_escritorio_apoio e senador_auxilio_moradia

Os três endpoints identificam o senador só pelo nome. O nome passa pelo cruzamento de `senador`, e
a tabela guarda só o `id_senador`; os dados de identificação ficam em `senador`. Nome que não está
no cruzamento sai com `id_senador` nulo. Os testes de `relationships` com `senador`, nessas três
tabelas e em `despesa_ceaps`, ignoram `id_senador` nulo.

### senador_aposentado_pensionista

Junta quatro endpoints com o mesmo assunto e formatos diferentes. A fonte não informa o regime em
nenhum campo: `regime` e `tipo_beneficio` registram de qual endpoint veio a linha.

| Endpoint | `tipo_beneficio` | `regime` | Campos |
|---|---|---|---|
| `/senadores/aposentados` | `aposentadoria` | `geral` | data de início e valor da remuneração |
| `/senadores/aposentados-ipc` | `aposentadoria` | `ipc` | data de início, sem valor |
| `/senadores/aposentados-pssc` | `aposentadoria` | `pssc` | data de início, sem valor |
| `/senadores/pensionistas-ipc` | `pensao` | `ipc` | datas de início e de fim, sem valor |

### servidor

Lê só `/servidores/servidores`, o quadro completo. Os endpoints de ativos, inativos, efetivos e
comissionados são filtros dessa lista por `situacao` e `vinculo`, que já são colunas da tabela.

### servidor_ativo

`/servidores` lista só os servidores ativos, com campos que `/servidores/servidores` não tem: data
de admissão, jornada semanal, afastamento, isenção de ponto e a hierarquia da lotação.

A hierarquia chega como uma lista que vai da raiz até a unidade do servidor. A última entrada é a
lotação; cada entrada tem o formato `SIGLA - Nome`, e a sigla vai para `sigla_lotacao`.
`hierarquia_lotacao` guarda o caminho inteiro, com as entradas separadas por ` / `.

A lista não tem identificador do servidor, só o nome: são 6.658 nomes distintos em 6.660 linhas. O
cruzamento com `servidor` só pode ser feito por nome.

### servidor_remuneracao

`id_remuneracao` (o `sequencial` da fonte) identifica o servidor no mês, não a linha: o mesmo código
aparece com uma folha `Normal` e uma `Suplementar` no mesmo mês. A chave é `ano`, `mes`,
`id_remuneracao` e `tipo_folha`. Linhas repetidas por inteiro na fonte (389 em 1,9 milhão) são
descartadas.

A folha de pensionistas não está na API: `/servidores/pensionistas/remuneracoes/{ano}/{mes}`
devolve o mesmo conteúdo que `/servidores/remuneracoes/{ano}/{mes}`.

### servidor_hora_extra e servidor_hora_extra_dia

`/servidores/horas-extras/{ano}/{mes}` devolve cada registro mensal com a lista de dias dentro.
`clean_servidor_hora_extra` separa as duas tabelas: `servidor_hora_extra` tem uma linha por
registro, com `valor_total`; `servidor_hora_extra_dia` tem uma linha por dia, com a quantidade de
horas e o setor. O valor é do mês inteiro e por isso fica só na tabela mensal: na tabela por dia,
ele se repetiria em cada dia do registro.

O mesmo `id_hora_extra` aparece mais de uma vez no mês consultado, com `mes_ano_prestacao`
diferentes. `mes_ano_prestacao` faz parte da chave das duas tabelas, e as linhas por dia também o
trazem, para ligar cada dia ao registro certo. As linhas repetidas na chave são descartadas.

`ano` e `mes` são o mês consultado na API.

### servidor_aposentado

Junta `/servidores/aposentados-efetivos` e `/servidores/aposentados-comissionados`. `tipo_quadro`
(`efetivo` ou `comissionado`) registra de qual endpoint veio a linha.

### servidor_cedido

Junta quatro endpoints, cada um com um conjunto diferente de campos. Nenhum deles informa o
sentido da cessão: o sentido vem de qual endpoint respondeu. `tipo_cessao` registra o endpoint, e
`orgao_origem` e `orgao_destino` são montados assim:

| Endpoint | `tipo_cessao` | `orgao_origem` | `orgao_destino` |
|---|---|---|---|
| `/servidores/cedidos/para-senado` | `cedido para o senado` | campo `orgaoOrigem` | `SENADO FEDERAL` |
| `/servidores/cedidos/pelo-senado` | `cedido pelo senado` | `SENADO FEDERAL` | campo `orgao` |
| `/servidores/cedidos/infraero-para-senado` | `cedido da infraero` | `INFRAERO` | `SENADO FEDERAL` |
| `/servidores/exercicio-provisorio` | `exercicio provisorio` | campo `orgaoOrigem` | `SENADO FEDERAL` |

### contratacao e contratacao_orgao_gestor

Uma contratação é um contrato, uma nota de empenho ou uma ata de registro de preço;
`tipo_contratacao` diz qual (`contratos`, `notas_empenho` ou `atas_registro_preco`).

Sem o filtro de status, `/contratacoes/contratos` devolve só parte dos contratos: 2.477, contra
8.162 com a consulta por status (5.685 `ENCERRADO`, 2.467 `VIGENTE` e 10 `EM_RENOVACAO`).
`fetch_contratacoes` faz uma chamada por status, com `statusContratoParam`, e junta o resultado; o
status consultado vai para a coluna `status`. Em `notas_empenho` (3.924) e `atas_registro_preco`
(656), a chamada sem filtro devolve o mesmo que a união dos status, e `status` fica nulo.

`id` só é único dentro do tipo: 577 ids aparecem em `contratos` e em `notas_empenho` como
contratações diferentes. A chave é o par `tipo_contratacao` e `id_contratacao`, na tabela-mãe e
nas filhas, como na rota dos sub-recursos (`/contratacoes/{tipo}/{id}/...`).

`contratacao_orgao_gestor` vem da lista `orgaos_gestores`, aninhada na resposta da contratação. O
`id` do órgão na fonte não é usado: em metade das linhas, ele repete o código da contratação (a
contratação 2412 lista dois órgãos, COPEGE e NGCIC, os dois com id 2412). O órgão é identificado
pela sigla, e as linhas repetidas por inteiro são descartadas.

### Filhas das contratações

As sete filhas só são extraídas no run semanal. Todas partem da mesma lista de `fetch_contratacoes`,
com uma chamada por contratação-mãe (caminhos relativos a `/contratacoes`):

| Tabela | Chamada | Para quais mães |
|---|---|---|
| `contratacao_item` | `/{tipo}/{id}/itens` | todas |
| `contratacao_garantia` | `/{tipo}/{id}/garantias` | todas |
| `contratacao_pagamento`, `contratacao_documento_fiscal` | `/{tipo}/{id}/pagamentos` | todas |
| `contratacao_pagamento_empenho` | `/{tipo}/{id}/pagamentos/{id_pagamento}/empenhos` | uma chamada por pagamento |
| `contrato_aditivo` | `/contratos/{id}/aditivos` | só contratos |
| `ata_acionamento` | `/atas_registro_preco/{id}/acionamentos` | só atas |

`contrato_aditivo` e `ata_acionamento` têm um tipo só e não trazem `tipo_contratacao`.
`contratacao` ganha uma partição `data_extracao` por dia; as filhas, uma por semana, com a data de
segunda-feira.

Numa amostra, nenhuma de 570 contratações das listas sem filtro de status tinha pagamento, e 15 de
120 contratos `ENCERRADO` tinham. Os contratos encerrados só entram na extração pela consulta por
status.

**Documentos fiscais.** Os documentos vêm aninhados em cada pagamento, mas a lista é a do contrato
inteiro, repetida em todos os pagamentos: o contrato 2280 devolve os mesmos 5 documentos em cada
um dos seus 4 pagamentos. `contratacao_documento_fiscal` fica no nível da contratação, sem
`id_pagamento`, e descarta as repetições pela chave `tipo_contratacao`, `id_contratacao`,
`id_documento_fiscal`. O endpoint próprio dos documentos fiscais devolve 0 linhas mesmo quando a
lista aninhada tem documentos, e não é lido.

**Empenhos dos pagamentos.** Os empenhos mudam de um pagamento para outro (o contrato 2280 tem 3
empenhos diferentes nos seus 4 pagamentos) e têm chamada própria, uma por pagamento. A fonte
repete alguns empenhos no mesmo pagamento: 111 chaves no histórico completo, todas com os mesmos
campos, menos uma, que difere só nas maiúsculas e minúsculas de `natureza_despesa`, com os mesmos
valores. A tabela fica com uma linha por `tipo_contratacao`, `id_contratacao`, `id_pagamento` e
`id_empenho`.

**Aditivos.** A fonte repete alguns aditivos por inteiro (34 chaves no histórico completo). A
tabela fica com uma linha por `id_contratacao` e `id_aditivo`.

### licitacao e licitacao_detalhamento

Os detalhamentos vêm aninhados em `/contratacoes/licitacoes` e formam `licitacao_detalhamento`.
`clean_licitacao` devolve as duas tabelas. O endpoint de detalhamento por licitação não é chamado,
o que evita uma requisição por licitação (2.743 na carga do histórico).

### empresa

Lê `/contratacoes/empresas` sem paginação, só com `id_empresa`, `cpf_cnpj` e `nome`.

A versão paginada (`?pagina=`) para na página 5 e repete registros entre páginas: 999 linhas e 774
ids distintos, contra 2.906 linhas e 2.903 ids distintos da chamada sem paginação. O único id que
só aparece na paginação (583) tem o mesmo CPF mascarado do id 200. A chamada sem paginação repete
três ids, e a tabela fica com uma linha por `id_empresa`.

Os campos `contratos`, `notas_empenho` e `atas_registro_preco` de cada empresa devolvem a mesma
lista combinada, sem `numero_formatado` e sem `unidade_gestora`. As contratações vêm dos próprios
endpoints.

### terceirizado

O campo `id` da fonte não identifica a pessoa: é igual a `codItem`, o item do contrato, e se repete
em todos os trabalhadores daquele item (um item tinha 137 pessoas). Ele não é lido. O item vai para
`id_item_contrato`, e a pessoa é identificada por `cpf` e `nome`.

### Supridos

Os seis `suprido_*` vêm de uma chamada por ano a `/supridos/{ano}`, que traz a árvore inteira:
suprido → atos de concessão → empenhos, transações (com objetos) e movimentações (com subtipos).
`clean_supridos` devolve as seis tabelas. Os quatro endpoints planos de supridos devolvem os mesmos
conjuntos (nos dados de 2024, os mesmos 464 empenhos e 1.362 transações) e não são lidos.

`ano` é o ano consultado; nos atos de concessão, é o `ano` do próprio ato, quando existe.

A fonte repete algumas movimentações por inteiro dentro do mesmo ato de concessão (3 de 836 em
2018), o que dobraria o valor delas. Os subtipos herdam a repetição. Cada tabela descarta as
repetições pela própria chave:

| Tabela | Chave |
|---|---|
| `suprido_ato_concessao` | `ano`, `codigo_ato_concessao` |
| `suprido_empenho` | `ano`, `codigo_ato_concessao`, `numero` |
| `suprido_transacao` | `ano`, `id_transacao` |
| `suprido_transacao_objeto` | `ano`, `id_transacao`, `id_objeto` |
| `suprido_movimentacao` | `ano`, `id_movimentacao` |
| `suprido_movimentacao_subtipo` | `ano`, `id_movimentacao`, `id_subtipo` |

O campo `tipoInscricaoTemp` não é lido: é o mesmo valor de `tipoInscricao` em forma de código
(`PESSOA_JURIDICA` em vez de `Pessoa jurídica`).

Na carga do histórico, `suprido_transacao` tinha dados de 2020 em diante, e `suprido_movimentacao`,
de 2013 a 2019. Ano sem registro não gera partição.

### quadro_pessoal

Junta seis relatórios de quantitativo de cargos e de pessoas, cada um com suas dimensões. Cada
linha é um relatório (`quadro`) × uma combinação de dimensões × um período, com cada medida na sua
coluna de contagem. As dimensões que o relatório não tem ficam nulas.

| Endpoint | `quadro` | Dimensões | Períodos |
|---|---|---|---|
| `/gestao/quadro-cargos-efetivos` | `cargo_efetivo` | `categoria`, `nivel`, `especialidade` | `ATUAL`, `ANTERIOR` |
| `/gestao/quadro-funcoes-comissionadas` | `funcao_comissionada` | `referencia` | `ATUAL`, `ANTERIOR` |
| `/servidores/quadro-servidores-estaveis-e-nao-estaveis` | `servidor_estavel` | `cargo` | `ATUAL`, `ANTERIOR` |
| `/servidores/quantitativos/pessoal` | `pessoal` | `classe`, `grupo`, `plano_carreira`, `nivel_escolaridade`, `padrao_nivel_referencia` | `ATUAL` |
| `/servidores/quantitativos/cargos-funcoes` | `cargo_funcao` | `nivel`, `tabela_vencimento` | `ATUAL` |
| `/senadores/quantitativos/senadores` | `senador` | `cargo` | `ATUAL` |

Os pares de campos `…Hoje` e `…Ant` (ou `…AnoAnterior`) viram duas linhas, com `periodo` igual a
`ATUAL` e `ANTERIOR`. Nem toda medida existe nos dois períodos: em `cargo_efetivo`,
`quantidade_cargos` só existe em `ATUAL`.

Os campos `variacao*` da fonte não são lidos. São a variação percentual entre os dois períodos e
saem das próprias colunas: 717 → 825 dá +15,1% e 528 → 475 dá −10,0%, os mesmos valores
publicados. Sem eles, todas as medidas são contagens inteiras.

`/senadores/quantitativos/senadores` é uma série histórica, não um retrato: 13 linhas que só
diferem pela data de referência. Essa data vai para `data_referencia`, que faz parte da chave.

### diretor_coordenador e previsao_aposentadoria

`diretor_coordenador` é a lista de setores com titular e substituto, com matrícula e e-mail.
`previsao_aposentadoria` é a projeção de aposentadorias por cargo e por ano e mês de direito. As
duas não são contagens do quadro atual e ficam fora de `quadro_pessoal`.

### dicionario

O `dicionario` cobre as colunas codificadas que o tratamento cria ou que a fonte guarda como
código:

- os indicadores `sim`/`nao`;
- `tipo_beneficio` e `regime` (`senador_aposentado_pensionista`), `tipo_quadro`
  (`servidor_aposentado`), `tipo_cessao` (`servidor_cedido`), `quadro` e `periodo`
  (`quadro_pessoal`);
- `tipo_contratacao`, nas sete tabelas que têm a coluna;
- `rubrica`, em `suprido_empenho`, `suprido_transacao_objeto` e `suprido_movimentacao_subtipo`.

Colunas que a fonte já entrega com rótulo legível (`vinculo`, `situacao`, `modalidade`,
`natureza_despesa` e outras) não entram.

As rubricas saem dos supridos extraídos no mesmo run. Só `suprido_empenho` traz a rubrica com
descrição; as outras duas tabelas trazem só o código e usam as descrições de `suprido_empenho`.
Rubrica sem descrição tem o próprio código como valor.

Os runs de rotina não carregam o `dicionario`, que só é refeito na carga completa. Rubrica que
aparece depois da última carga completa não está no `dicionario`.

## Testes

`tests/test_source_defects.py` testa os tratamentos sem acessar a API, com dados no formato das
respostas reais:

- leitura de datas, sentinelas de nulo, valores, booleanos e horas;
- documentos fiscais no nível da contratação;
- movimentações repetidas nos supridos;
- sentido da cessão em `servidor_cedido`;
- consolidação de `quadro_pessoal` e variação percentual derivada dos dois períodos;
- gravação ano a ano e tabela vazia sem parquet;
- limite de falhas de `fan_out`;
- normalização de nomes e cruzamento de `senador`;
- `COVERAGE` de cada tabela, sem faixa paga invertida.

```bash
uv run pytest pipelines/datasets/br_senado_dados_abertos_administrativos/tests/
```

Os testes não consultam a API: se a fonte corrigir um defeito, eles continuam passando.
