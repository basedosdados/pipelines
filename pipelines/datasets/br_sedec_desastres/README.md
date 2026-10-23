# Documentação do Conjunto de Dados: br_sedec_desastres

Conjunto criado a partir da issue
[#1747](https://github.com/basedosdados/pipelines/issues/1747). O flow roda todo dia
em prod e acrescenta o retrato do dia a
`basedosdados.br_sedec_desastres.reconhecimentos_vigentes`. O dataset está publicado.
A segunda tabela, `reconhecimentos_realizados`, tem seção própria.

## Sobre o Sistema

O **S2ID (Sistema Integrado de Informações sobre Desastres)** é a base
administrativa oficial do Governo Federal para registro e gestão de desastres
ocorridos no país. É mantido pela **SEDEC (Secretaria Nacional de Proteção e
Defesa Civil)**, vinculada ao **MIDR (Ministério da Integração e do
Desenvolvimento Regional)**.

Cada tabela do conjunto vem de um relatório gerencial da mesma página:

- `reconhecimentos_vigentes`, do relatório **"Reconhecimentos vigentes"**: os
  reconhecimentos federais de situação de emergência e de estado de calamidade
  pública **em vigor**;
- `reconhecimentos_realizados`, do relatório **"Reconhecimentos realizados"**: o
  histórico das portarias de reconhecimento desde 2013.

**Página da fonte:** `https://s2id.mi.gov.br/paginas/relatorios/`

Fora a seção "A tabela `reconhecimentos_realizados`" e a "Estrutura", que cobre as
duas, este arquivo descreve a `reconhecimentos_vigentes`.

## Decisões

As quatro decisões abaixo estão fechadas.

### 1. `dataset_id`: `br_sedec_desastres` × o `s2id` que já existe

A issue pede `br_sedec_desastres`, mas o backend **de prod** já tem um dataset de
slug **`s2id`** (organização `midr`, temas `agriculture` + `environment`, tags
`disaster`/`natural-disaster`/`nature`), **sem nenhuma tabela**. Pela convenção
`<org>_<slug>` o id GCP dele seria `br_midr_s2id`.

O conjunto é registrado como dataset novo, `br_sedec_desastres`. O `s2id` de prod
permanece sem tabelas, de modo que há dois datasets para a mesma fonte no backend;
arquivar o `s2id` é trabalho pendente.

Pela convenção `<org>_<slug>`, esse nome exige uma organização `sedec`, que não
existia no backend e foi criada. O dataset tem slug `desastres` sob a org `sedec`, e
o id do BigQuery é `br_sedec_desastres`.

### 2. Retrato × histórico: a tabela acumula retratos

"Vigentes" é um **retrato do momento**: quando um reconhecimento vence, a linha
desaparece da fonte.

A tabela acumula esses retratos. Cada execução grava o retrato inteiro com a data da
extração, e a tabela é a série desses retratos.

O que isso fixa:

| Aspecto | Valor |
| --- | --- |
| `dump_mode` | `append` |
| partição | `data_extracao` (DATE) |
| chave única | `data_extracao` + a chave do reconhecimento |
| volume | ~1239 linhas por retrato (medido em 2026-08-05) |

Duas consequências:

**A série começa no primeiro run.** Não há como recuperar retratos passados, porque
a fonte só mostra o presente: no primeiro run a tabela tem um dia de história, e a
série cresce a partir dele.

**O guarda do poll exige tratamento específico.** `poll_source_for_update_task`
compara a data máxima da fonte com o fim do `Coverage.DateTimeRange` da tabela, para
não reingerir dado que não mudou. Numa série de retratos a fonte não publica data de
referência, então o que alimenta a comparação é a `data_extracao` estampada por
`clean_all` — a mesma coluna declarada no `_COVERAGE`, o que deixa os dois lados
sendo data de retrato: um retrato novo passa, um segundo run no mesmo dia não.

### 3. Frequência: diária

**A fonte não tem frequência de publicação.** Ela é contínua: o reconhecimento
federal sai por portaria publicada no DOU em dia útil qualquer, depois de o
município decretar, registrar no S2ID e a SEDEC analisar. Em 2026-08-05, Minas
Gerais passou de 136 para 147 linhas em duas horas.

A frequência não é da fonte, e sim a cadência de retrato do flow: **diária**, às
02:10 (`10 2 * * *`, `America/Sao_Paulo`).

A vigência legal do reconhecimento é de **180 dias** a contar da publicação do
decreto, prorrogável — nas linhas medidas, o intervalo entre ocorrência e vigência
tem mínimo de 180 e máximo de 326 dias. Com um retrato por dia, cada reconhecimento
aparece em pelo menos 180 retratos consecutivos, e a entrada e a saída de cada um
ficam registradas com precisão de um dia.

O volume é de cerca de 1.200 linhas por retrato, uns 440 mil por ano. Os três
primeiros retratos da série, de 2026-08-06, 2026-09-02 e 2026-09-25, têm intervalos
irregulares.

Duas observações sobre a periodicidade que a fonte declara:

- o portal de dados abertos do MDR (`dadosabertos.mdr.gov.br/dataset/s2id_sedec`)
  declara periodicidade **"Mensal"**, mas isso descreve **outro** conjunto: são CSVs
  anuais de "Danos Informados" 2013-2022, sem recurso de reconhecimentos, e o portal
  está **parado desde agosto de 2024**;
- é essa a raw data source registrada no backend para o dataset `s2id`, e ela não
  aponta para a fonte desta tabela.

A cadência diária aciona a regra da janela BD Pro, que vale para tabela atualizada
mensalmente ou com mais frequência. O tier adotado no `_COVERAGE` de `flows.py` é
`AllFree`, uma exceção a essa regra: numa série de retratos, o paywall da janela
recente restringiria ao BD Pro o **estado atual** dos reconhecimentos e liberaria
apenas os retratos com mais de seis meses. Em termos de registro, isso significa uma
Coverage, `is_closed=False`, e nenhuma Row Access Policy.

### 4. Onde fica o join do município: no dbt

A fonte dá o **nome** do município, e a convenção exige `id_municipio` com FK
para o diretório.

O join fica no modelo dbt, que é o padrão do repo para join contra produção. O
staging carrega `nome_municipio`; `id_municipio` só existe na tabela final, e o nome
não sobrevive a ela, já que vive no diretório.

Consequências no código:

- `reconhecimentos_vigentes.STAGING_SUBSTITUICOES` declara a divergência
  (`id_municipio` → `nome_municipio`), e o `write_partitioned` a aplica ao montar
  a ordem das colunas do parquet a partir da arquitetura;
- a normalização de nome e o mapa de municípios renomeados **não** ficam em
  Python: viram SQL, no formato do modelo do BNDES;
- o `not_null` em `id_municipio` no `schema.yml` é o que impede perda silenciosa,
  porque o `left join` devolve NULL para quem não casar.

As seis exceções e o tratamento do acento grave estão no `case` do próprio `.sql`,
detalhados na seção "O join do município" abaixo.

## O modelo dbt

O `.sql` e o `schema.yml` são enxutos; o raciocínio por trás deles está descrito
aqui.

### O join do município

Três CTEs: `staging` (safe_cast das 8 colunas), `municipio` (o diretório) e
`staging_norm` (o lado da fonte, corrigido e normalizado). O select final junta
com `left join` em `sigla_uf` **e** `nome_norm`.

**As duas expressões de normalização precisam ser idênticas** — `municipio` e
`staging_norm` aplicam as mesmas quatro operações na mesma ordem. Se divergirem,
não há erro: o resultado é `id_municipio` nulo.

A ordem é: corrigir a grafia → hífen para espaço → `upper` →
`regexp_replace(normalize(..., nfd), r'[^A-Z0-9 ]', '')`. A última etapa resolve
acento **e** apóstrofo de uma vez, porque depois do `nfd` a marca combinante é um
caractere próprio e cai junto com o acento grave e o apóstrofo. O BNDES, que é o
modelo copiado, usa `r'\pM'` no lugar, que casa só marca combinante e deixa o
apóstrofo passar — é o que fazia 10 dos 16 nomes não casarem.

**O `case` das exceções é escopado por UF**, diferente do BNDES, que compara só o
nome. Cada uma das 6 foi medida num estado específico; sem o `sigla_uf =`, um
homônimo em outro estado com a grafia correta seria reescrito para a errada. Não
há colisão hoje, mas a lista vai crescer.

**`left`, não `inner`.** `inner` descartaria a linha cujo município não casou, e a
tabela sairia menor sem nenhum sinal. `left` mantém a linha com `id_municipio` nulo,
que o teste `not_null` acusa.

### Os testes, e de que falha cada um protege

| O que pode dar errado | Efeito | Quem pega |
| --- | --- | --- |
| município não casa no join | `id_municipio` nulo | `not_null` em `id_municipio` |
| dois municípios da mesma UF colapsam no mesmo `nome_norm` | linha duplicada | `unique_combination_of_columns` |

O `not_null` em `id_municipio` é o teste que transforma um município ausente do
diretório em falha visível.

A chave única foi medida em 2026-08-05: `(UF, município, COBRADE, data de
ocorrência)` dava 1239 valores únicos em 1239 linhas. `data_extracao` entra por
ser o que distingue um retrato do outro.

No `relationships` de `sigla_uf`, o campo do diretório é **`sigla`**, não
`sigla_uf`: a tabela `br_bd_diretorios_brasil.uf` tem `id_uf`, `nome`, `regiao` e
`sigla`. É também o valor do `directory_column` na arquitetura — um
`directory_column` que não resolve faz a coluna ser descartada sem erro no registro
das colunas.

## Notas sobre a fonte

O `/paginas/relatorios/` é uma aplicação **JSF/PrimeFaces**: o export não sai de
uma URL de download estável, e sim de um POST no formulário da página, com
sessão e `ViewState`. Os ids dos componentes são gerados pelo framework e mudam
quando a página muda. Os XPaths em `reconhecimentos_vigentes.XPATHS` são mistos: o painel e o
checkbox de tipologias são ancorados por texto, enquanto o widget de estado, o select
oculto e o botão de export usam os ids `abas:sanfonas:*`. Esses três estão acoplados
ao HTML da fonte e precisam ser revalidados quando a página mudar.

O painel "Reconhecimentos vigentes" oferece PDF, XLS e CSV.

### O export da vigentes

O CSV é um relatório, não uma tabela simples:

- quatro linhas de preâmbulo (ministério, secretaria, título do relatório e
  `UF: XX`) antes do cabeçalho, que começa com `UF;`;
- toda linha de dados termina com um `;` a mais, o que dá 7 campos contra 6 do
  cabeçalho. Lida sem tratamento, falha com `found more fields than defined in
  'Schema'`, e por isso o `_parse_export` corta esse `;`;
- no fim, uma linha vazia e o rodapé `Total de reconhecimentos vigentes: ;N;`.

O `N` do rodapé é a contagem da própria fonte, e o `build_reconhecimentos_vigentes`
confere com ele as linhas lidas de cada arquivo.

### Edge cases conhecidos

**UF sem nenhum reconhecimento vigente.** Em 2026-08-05, DF e ES vieram com zero
linhas: arquivo de 263 bytes, só cabeçalho e o rodapé
`Total de reconhecimentos vigentes: ;0;`. O arquivo é válido,
não é erro de download. Por isso as validações checam que os 27 arquivos existem, e
não que toda UF tem linha.

**Retrato vazio no país inteiro.** Não observado, e improvável: há sempre
reconhecimento em vigor em algum município. Nesse caso o `df` sairia vazio e o
`df["data_extracao"].max()` do `clean_all` devolveria `NaT`, quebrando no `.strftime`
com `AttributeError: 'NaTType' object has no attribute 'strftime'`. Não há tratamento
no código.

**COBRADE com formato diferente.** Hoje os 19 valores distintos seguem
`NNNNN - Rótulo`, e o `splitn(" - ", 2)` depende disso. Um valor sem `" - "`
deixaria `nome_cobrade` nulo e `id_cobrade` com o texto inteiro — e `id_cobrade`
faz parte da chave da tabela.

### Raspagem: selenium headless

A alternativa era `requests.Session` + BeautifulSoup, montando o postback
manualmente: ler o `ViewState` e remontar os campos do formulário. A raspagem usa
browser, o que deixa `ViewState`, sessão e postback a cargo do Chrome.

A imagem do repo suporta: o `Dockerfile` instala `google-chrome-stable` e
`webdriver-manager` é dependência pinada. Precedentes de selenium no repo:
`pipelines/datasets/br_stf_corte_aberta/utils.py` e `pipelines/shared/bcb/utils.py`,
ambos esperando com `time.sleep` fixo. Aqui a espera é por `WebDriverWait` e por
`_wait_for_download`, que sonda o diretório até o arquivo fechar.

Os detalhes abaixo valem para as duas tabelas.

**O que conta como download pronto.** O `_wait_for_download` só aceita um `.csv`
que não esteja em `seen` e só quando nada está em andamento. Em andamento conta o
`<nome>.crdownload` e também os temporários ocultos `.com.google.Chrome.*` e
`.org.chromium.*`, que o Chrome grava e depois renomeia. Tomado por pronto, o
temporário some antes de ser renomeado, e o export de verdade sobra no diretório,
onde é confundido com o do export seguinte. O `seen` existe porque os exports saem
em laço: a partir da segunda volta já há um arquivo pronto no diretório, e
"qualquer arquivo completo" devolveria o anterior.

**Cada tentativa começa com o diretório de entrada vazio.** O retry da task
reaproveita o mesmo diretório com um `seen` novo, e um CSV que sobrou da tentativa
anterior seria devolvido como o download novo e renomeado com o nome errado. Na
`vigentes`, a checagem de `UF` contra o nome do arquivo acusaria isso; mesmo assim,
sem limpar o diretório, o retry não teria como dar certo.

**A pré-checagem da fonte.** Antes de abrir o Chrome, o `_checar_fonte` faz um GET
simples na raiz do site e na página dos relatórios e registra status, tamanho e
cabeçalhos. Ele nunca falha. Serve para ler um timeout do Selenium:

- se a pré-checagem passa e o Chrome não, quem é barrado é o navegador, e a saída
  seria montar o postback com `requests`;
- se a raiz e a página dos relatórios dão o mesmo erro, quem é barrado é o IP de
  saída, e o código não tem como contornar;
- se só a página dos relatórios cai, a regra é desse caminho, e vale investigar
  sessão, `Referer` e ordem de navegação.

Resposta com menos de 2.000 bytes (`_HTML_MINIMO`) não é o relatório: a página
passa de 250 KB, e a página de bloqueio que o pod recebeu tinha 165 bytes. Nesse
caso o corpo inteiro vai para o log, na pré-checagem e logo depois de o Chrome abrir
a página.

Restrições que vêm com a escolha:

- `--disable-dev-shm-usage` é obrigatório no k8s, porque o `/dev/shm` do pod é
  pequeno;
- o `job_variables = {"memory": "4Gi"}` em `flows.py` não foi medido;
- a página tem **vários** botões "Exportar CSV", um por relatório. O seletor precisa
  ser relativo ao painel certo, senão baixa o relatório errado, sem erro.

### O proxy brasileiro, e por que há um repasse local

O S2ID recusa IP estrangeiro: responde `403` com `Acesso bloqueado por localizacao
geografica`, e o cluster roda em `us-central1`. A saída é o Squid em
`southamerica-east1` (`iac#156`), lido de `BRASIL_PROXY_URL` pelos helpers
`brasil_proxy_url()`/`brasil_proxy_dict()`.

Nas chamadas de `requests` isso é um argumento e acabou. Com o Chrome não: ele só
aceita proxy pelo `--proxy-server`, essa flag não tem campo para credencial, e o
Squid exige Basic Auth — é o único controle de acesso dele, sem allowlist de IP.

As quatro formas de dar a credencial ao Chrome, todas medidas contra um Squid de
mentira que exige Basic Auth:

| tentativa | resultado |
|---|---|
| credencial na flag (`http://user:senha@host:porta`) | `net::ERR_NO_SUPPORTED_PROXIES`; a flag é rejeitada inteira e não sai pedido |
| flag sem credencial, com túnel TCP no meio (`socat`, `ssh -L`) | 12 × 407: cano cego não insere cabeçalho |
| extensão tratando `onAuthRequired` | 12 × 407: `--load-extension` está desativado desde o Chrome 137, e o pod roda 153 |
| `selenium-wire` | ignora o proxy e vai direto; além disso só importa com `setuptools<81`, `blinker==1.7` e `pyopenssl<23.3` |
| repasse local (`_proxy_local`) | 0 × 407, 23 pedidos autenticados, página carregada |

Daí o repasse: o Chrome aponta para `127.0.0.1`, que não pede autenticação; ele abre
a conexão com o Squid acrescentando o `Proxy-Authorization` e, a partir da primeira
linha, só copia bytes. O `CONNECT` do HTTPS atravessa inteiro, então o TLS segue
ponta a ponta entre o Chrome e a fonte — nem o repasse nem o Squid veem o conteúdo.

Custo medido: **0,5 MB** de RSS a mais, duas threads por conexão viva, e **0,02 s** de
CPU a cada 64 MB. O Chrome sozinho passa de 300 MB.

Ele desaparece no dia em que o Squid liberar a origem do cluster por IP, e aí basta
`--proxy-server=http://host:3128`.

## A tabela `reconhecimentos_realizados`

A tabela vem do relatório **"Reconhecimentos realizados"**: as portarias federais de
reconhecimento de situação de emergência e de estado de calamidade pública, com o
decreto, a tipologia COBRADE e a publicação no DOU. Ao contrário da `vigentes`, é um
histórico: o relatório aceita qualquer período a partir de 01/01/2013.

### O painel

O painel tem três filtros obrigatórios: período, tipologias e estados. A raspagem
marca "Todas as tipologias de desastres" e "Todas os estados" (grafado assim na
fonte), e cada export cobre o país inteiro, em vez de um arquivo por UF como na
`vigentes`.

Cada consulta aceita no máximo 365 dias. A raspagem pede um semestre por vez, de
01/01 a 30/06 e de 01/07 a 31/12, o que respeita o limite mesmo em ano bissexto. A
última janela termina no dia da execução, porque o calendário não aceita data futura.

Os dois campos de data são somente leitura e abrem um calendário. A raspagem escreve
a data direto no `value` do campo; como o export é um postback do formulário
inteiro, o valor vai junto.

O botão "Exportar CSV" deste painel tem o id `btnExportarCsv`. Os XPaths ficam em
`reconhecimentos_realizados.XPATHS` e seguem o critério da `vigentes`: ancorados no texto e
restritos ao painel.

### O export

O arquivo tem três linhas de preâmbulo (a última traz o período consultado), o
cabeçalho `Nº;UF;Código IBGE;Município;...` e dois rodapés, `Total de municípios
reconhecidos` e `Total de reconhecimentos realizados`. O segundo rodapé serve para
conferir a contagem de linhas de cada arquivo.

O export já traz o código IBGE, então `id_municipio` sai direto dele, sem o join por
nome que a `vigentes` faz. Ficam fora da tabela o `Nº`, que numera as linhas do
relatório, e o nome do município, que está no diretório.

A coluna `Desastre` traz só o nome da tipologia, em maiúsculas e sem o código
COBRADE, ao contrário do `COBRADE` da `vigentes`, que vem como `12300 - Alagamentos`.
O código sai de `reconhecimentos_realizados.COBRADE`, as 65 tipologias do filtro "Desastre"
do próprio painel, copiadas dos rótulos `código - nome`. Os dois lados são comparados
em maiúsculas, sem acento e só com letras e dígitos, o que absorve erros de grafia da
fonte como `ROMPIMENTO/COLAPSO DEBARRAGENS`. O `nome_cobrade` vem do painel, então um
mesmo código tem o mesmo nome nas duas tabelas. Um nome sem código interrompe a
limpeza; a correção é acrescentar a entrada em `COBRADE`.

### Linhas repetidas e ausência de chave única

A fonte repete algumas linhas de forma idêntica. As cópias vêm do próprio relatório:
cada arquivo bate com o próprio rodapé, e as janelas de consulta não se sobrepõem. A
tabela mantém as cópias para bater com a fonte, e por isso o `schema.yml` não tem
teste de chave única.

Cópias idênticas na carga inicial, de 2013 a 2025:

| ano | linhas | cópias idênticas |
| --- | --- | --- |
| 2013 | 684 | 1 |
| 2014 | 1.189 | 1 |
| 2017 | 2.898 | 1 |
| 2018 | 2.289 | 3 |
| 2019 | 2.470 | 295 |
| 2020 | 10.342 | 876 |
| 2021 | 8.552 | 331 |
| 2022 | 3.644 | 10 |
| 2023 | 3.195 | 38 |
| 2024 | 3.434 | 3 |
| **total (2013–2025)** | **44.853** | **1.559** |

Os anos fora da tabela (2015, 2016 e 2025) não têm cópias.

Fora as cópias, a combinação `ano`, `id_municipio`, `id_cobrade`, `numero_portaria`,
`numero_decreto` e `data_dou` identifica um reconhecimento, com uma exceção: duas
linhas distintas de 2023 têm a mesma combinação. A `data_dou` entra porque o
`numero_decreto` e o `numero_portaria` podem vir nulos, como nas linhas conferidas de
2013, e aí um município reconhecido duas vezes no mesmo ano pela mesma tipologia só
se distingue pela publicação. Sem ela, 262 linhas distintas colidiam: 49 em 2013, 210 em 2014, 2
em 2015 e 1 em 2023.

### Partição, carga e atualização

A partição é `ano`, o ano da janela de consulta. O relatório filtra o período pela
data de publicação da portaria no Diário Oficial da União, então `ano` é sempre o ano
da `data_dou`; a `data_portaria` pode cair no ano anterior. Cada ano vai para
`ano=<ano>/data.parquet`. Recarregar um ano sobrescreve esse arquivo no staging sem
duplicar linhas: o `dump_mode` é `"append"`, mas o nome do arquivo é o mesmo.

**Carga inicial.** O `run_local_reconhecimentos_realizados.py` baixa de 2013 a 2025, um ano por vez, e pula
os anos que já têm partição em `tmp/br_sedec_desastres/reconhecimentos_realizados/`.
Com `--upload`, sobe tudo para o staging de `basedosdados-dev` ao final.

**Atualização.** O `check_update` roda todo dia às 02:50 (`50 2 * * *`,
`America/Sao_Paulo`), baixa o ano corrente e compara a maior `data_dou` com o fim da
cobertura. Em janeiro, baixa também o ano anterior, para não perder as portarias
publicadas nos últimos dias de dezembro. Se houver portaria nova, o `extract_and_load`
baixa os mesmos anos de novo, e o `build_and_promote` promove só essas partições.

A tabela é toda livre: `reconhecimentos_realizados.COVERAGE` é `AllFree`, com uma
Coverage só (`is_closed=False`) e nenhuma Row Access Policy.

**Backfill.** Disparar à mão o deployment
`extract_and_load: br_sedec_desastres__reconhecimentos_realizados/br_sedec_desastres_reconhecimentos_realizados_extract_and_load`
com os anos nos parâmetros (no pool de dev, o nome do deployment leva o prefixo
`dev-`):

```json
{"download_params": {"years": [2019, 2020]}}
```

Sem `years`, o run carrega os mesmos anos do run diário.

## Estrutura

```text
pipelines/datasets/br_sedec_desastres/
├── constants.py                            URLs, XPaths, timeouts e COLUNAS (o schema) de cada tabela
├── utils.py                                download e limpeza (funções puras, sem Prefect)
├── tasks.py                                @task sobre as funções de utils, com os retries
├── flows.py                                os @flow de cada tabela, com o schedule
├── run_local_reconhecimentos_vigentes.py   roda as etapas da vigentes na máquina local
├── run_local_reconhecimentos_realizados.py carga inicial da realizados
└── README.md                               este arquivo

models/br_sedec_desastres/
├── br_sedec_desastres__reconhecimentos_vigentes.sql    modelo dbt
├── br_sedec_desastres__reconhecimentos_realizados.sql  modelo dbt
└── schema.yml                                          testes
```

Uma base brasileira não tem `code/` sob `models/`: dos 158 diretórios em `models/`,
18 têm `code/architecture/`, e nenhum deles é base de dados brasileira — dois são
diretórios de outros países. Arquitetura commitada, script de limpeza local e
`upload.py` são o padrão de onboarding **internacional**.

Em consequência:

- **a planilha de arquitetura não é versionada**, em **xlsx** (o padrão), com uma
  cópia como Planilha Google no Drive. Como o código não pode lê-la, o schema
  (ordem, tipos, `original_name`) vive no `COLUNAS` da classe de cada tabela no `constants.py`, e toda alteração na
  planilha precisa ser refletida lá;
- **não há carga inicial separada.** Todo retrato sai das mesmas tasks de download
  e limpeza, e a série começa no primeiro retrato (decisão 2);
- as etapas são executáveis localmente pelo `run_local_reconhecimentos_vigentes.py`: ele chama as mesmas
  `@task` do flow via `.fn()` e escreve em `tmp/br_sedec_desastres/` (ver
  "Atualização diária" abaixo). O que ele não cobre é a fiação do `flows.py`: a
  ordem das etapas, o guarda do poll e os retornos antecipados.

## Atualização diária

O flow `br_sedec_desastres__reconhecimentos_vigentes` roda no pool de prod todo dia
às 02:10 (`10 2 * * *`, `America/Sao_Paulo`), com o download saindo pelo proxy
brasileiro. Cada run:

1. baixa os 27 exports e monta o retrato, com `data_extracao` igual à data da
   execução;
2. grava o Poll da fonte e compara a data do retrato com o fim da cobertura; se o
   dia já tem retrato, o run termina aqui (decisão 2);
3. grava o Update da fonte com a data do retrato;
4. acrescenta o retrato ao prefixo `staging/br_sedec_desastres/reconhecimentos_vigentes/`
   do bucket `basedosdados` (`dump_mode="append"`);
5. roda `dbt run` e `dbt test` em prod;
6. reescreve a cobertura e o Update da tabela.

Um run que termina no passo 2 também fica `COMPLETED`, então o estado do run não
prova ingestão. A ingestão se confere no log, onde
`Não há novas atualizações na fonte original` marca o run que não ingeriu, ou na
data máxima da tabela.

**O prefixo de staging no bucket `basedosdados` é o histórico da série.** Os retratos
diários existem só ali. O prefixo de mesmo nome em `basedosdados-dev` não acompanha
o de prod e tem retratos de teste, gravados pelas validações em dev.

**Retrato não gerado é retrato perdido.** A fonte publica apenas o estado atual. Um
dia em que o flow não roda, ou falha antes do upload, fica sem retrato, e esse
retrato não se recupera depois: a série fica com um buraco de um dia.

### Esta tabela não usa o `table-approve`

No merge de uma PR com o rótulo `table-approve`, a action age sobre toda tabela cujo
`.sql` mudou na PR. Para cada uma, o `push_table_to_bq` de
`.github/workflows/scripts/prefect_run_dbt.py` espelha o prefixo de staging do bucket
`basedosdados-dev` no `basedosdados`: apaga o que havia em prod, com backup em
`basedosdados-backup`, e copia o que está em dev.

Nesta tabela, o espelhamento deixa prod igual a dev: some todo retrato que só existe
em prod, e os retratos de teste de dev vão ao ar. O backup guarda uma versão só,
porque cada espelhamento apaga o backup anterior antes de gravar o novo.

Antes de qualquer PR com o rótulo `table-approve` que altere
`models/br_sedec_desastres/br_sedec_desastres__reconhecimentos_vigentes.sql`, o
prefixo de dev precisa ter exatamente os mesmos arquivos que o de prod.

### Armar, rodar à mão e validar em dev

**Armar.** O merge implanta o deployment de prod pausado. O schedule só vale depois
de marcar `is_schedule_active` na linha do `flow_name`
`br_sedec_desastres__reconhecimentos_vigentes` em
`https://backend.basedosdados.org/admin/admin_data_tools/disabledflowschedule/`;
desmarcar pausa o deployment de novo. Um deployment pausado continua exibindo o
schedule como ativo; o estado real está no campo `paused` do deployment.

**Rodar à mão.** Disparar o deployment
`br_sedec_desastres__reconhecimentos_vigentes/br_sedec_desastres__reconhecimentos_vigentes`
com os parâmetros padrão, `{}`, que equivalem a `materialize_after_dump=True`,
`update_metadata=True` e `target="prod"`. Se o dia já tem retrato, o poll encerra o
run no passo 2.

**Validar em dev.** A PR com o rótulo `deploy-flow` implanta o deployment
`dev-br_sedec_desastres__reconhecimentos_vigentes`, disparado com:

```json
{"materialize_after_dump": false, "update_metadata": false, "force_run": true}
```

`force_run` pula o poll, e `materialize_after_dump=false` desvia o run para dev: o
retrato vai para o prefixo de staging de `basedosdados-dev`, o `dbt run` e o
`dbt test` rodam em dev, e o Update da fonte não é gravado. Nenhum metadado de prod
muda. O rótulo só implanta o código de uma PR que altera o `flows.py`; nas outras, o
deployment roda a branch que já tinha, e o caminho do clone no log
(`/app/pipelines-<branch>/`) mostra qual foi.

### `run_local_reconhecimentos_vigentes.py`

Roda as etapas do flow na máquina local, chamando as mesmas `@task` via `.fn()`, sem
o runtime do Prefect, e grava em `tmp/br_sedec_desastres/`. Serve para depurar. Sem
`BRASIL_PROXY_URL` definida, o download sai direto, o que exige IP brasileiro.

| etapa | onde escreve |
| --- | --- |
| `download`, `clean` (padrão) | só em `tmp/br_sedec_desastres/` |
| `upload` | prefixo de staging em `basedosdados-dev` |
| `dbt` | `dbt run` e `dbt test` em dev |
| `metadata` | backend de **prod**: Poll, Update da fonte, cobertura e Update da tabela |

Nenhuma etapa grava dado em prod. A etapa `metadata` só escreve se a data máxima da
tabela de prod for igual à do retrato local, e repete o que o run diário já grava.
Ela precisa de Python 3.11: importa o `_COVERAGE` do `flows.py`, que depende de
`pipelines/utils/metadata/domain.py`, e esse módulo usa `enum.StrEnum`. Em venv 3.10
o import falha (`uv venv --python 3.11`).

## O que falta

- [ ] Trocar o `entity` do Update da tabela e do Update da fonte, registrados em prod
      como `month`, para `day`, com `frequency=1`. Num Update que já existe, o flow
      reescreve só o `latest`, então a troca feita à mão se mantém.
- [ ] Arquivar o dataset `s2id` de prod, que não tem tabelas (decisão 1).
