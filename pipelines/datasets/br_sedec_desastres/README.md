# Documentação do Conjunto de Dados: br_sedec_desastres

Conjunto criado a partir da issue
[#1747](https://github.com/basedosdados/pipelines/issues/1747). O pipeline está
validado de ponta a ponta em dev: 27 downloads, limpeza, upload para
`basedosdados-dev`, `dbt run` e `dbt test`. Os metadados estão registrados em prod
com `status = under_review`.

## Sobre o Sistema

O **S2ID (Sistema Integrado de Informações sobre Desastres)** é a base
administrativa oficial do Governo Federal para registro e gestão de desastres
ocorridos no país. É mantido pela **SEDEC (Secretaria Nacional de Proteção e
Defesa Civil)**, vinculada ao **MIDR (Ministério da Integração e do
Desenvolvimento Regional)**.

A tabela deste conjunto vem do relatório gerencial **"Reconhecimentos vigentes"**:
os reconhecimentos federais de situação de emergência e estado de calamidade
pública **em vigor**.

- **Página da fonte:** `https://s2id.mi.gov.br/paginas/relatorios/`
- **Tabela:** `reconhecimentos_vigentes`

## Decisões

As cinco decisões abaixo estão fechadas.

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
o id do BigQuery é `br_sedec_desastres`. Ver a seção Metadados.

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
compara a data máxima da fonte com o `Update` registrado, para não reingerir dado
que não mudou. Numa série de retratos, todo run produz linhas novas — outra
`data_extracao` — e a fonte não publica data de referência. Ver a decisão 4.

### 3. Frequência: mensal

**A fonte não tem frequência de publicação.** Ela é contínua: o reconhecimento
federal sai por portaria publicada no DOU em dia útil qualquer, depois de o
município decretar, registrar no S2ID e a SEDEC analisar. Em 2026-08-05, Minas
Gerais passou de 136 para 147 linhas em duas horas.

A frequência não é da fonte, e sim a cadência de retrato adotada aqui: **mensal**.

A vigência legal do reconhecimento é de **180 dias** a contar da publicação do
decreto, prorrogável — nas linhas medidas, o intervalo entre ocorrência e vigência
tem mínimo de 180 e máximo de 326 dias. Como nenhum reconhecimento vive menos de 180
dias, **um retrato por mês captura todos**, e cada um aparece em cerca de seis
retratos consecutivos. Cadência diária acrescentaria apenas precisão sobre o dia de
entrada e saída, a 30 vezes o volume: ~450 mil linhas por ano contra ~15 mil.

Duas observações sobre a periodicidade que a fonte declara:

- o portal de dados abertos do MDR (`dadosabertos.mdr.gov.br/dataset/s2id_sedec`)
  declara periodicidade **"Mensal"**, mas isso descreve **outro** conjunto: são CSVs
  anuais de "Danos Informados" 2013-2022, sem recurso de reconhecimentos, e o portal
  está **parado desde agosto de 2024**;
- é essa a raw data source registrada no backend para o dataset `s2id`, e ela não
  aponta para a fonte desta tabela.

Cadência mensal aciona a regra da janela BD Pro, que vale para tabela atualizada
mensalmente ou com mais frequência. O tier adotado no `_COVERAGE` de `flows.py` é
`AllFree`, uma exceção a essa regra: numa série de retratos, o paywall da janela
recente restringiria ao BD Pro o **estado atual** dos reconhecimentos e liberaria
apenas os retratos com mais de seis meses. Em termos de registro, isso significa uma
Coverage, `is_closed=False`, e nenhuma Row Access Policy.

### 4. O que alimenta o poll — a `data_extracao`, com o guarda mantido

`poll_source_for_update_task` compara a data máxima da fonte com o
`Table.Update.latest` e, se não houver novidade, o flow retorna sem materializar.
O flow mantém esse guarda.

O `max_date` que alimenta a comparação é a própria `data_extracao` estampada por
`clean_all`, e não um `date.today()` recalculado no flow, que divergiria se o run
atravessasse a meia-noite.

O guarda não congela esta tabela, ao contrário do que ocorre em séries de cobertura
defasada como a do `br_ibge_ipca`. O `Table.Update.latest` é escrito por
`register_table_materialization` como o `bq.last_modified` da tabela, isto é, um
relógio; o congelamento acontece quando o `max_date` é uma data de cobertura sempre
anterior a esse relógio. Aqui a `data_extracao` é estampada no momento do run, então
no run seguinte ela é posterior ao `last_modified` do anterior. Como
`MetadataClient._read_update_latest` trunca o valor em `[:10]` e devolve `date`, a
comparação é `date × date`, e o guarda só bloqueia um segundo run no mesmo dia.

O primeiro run é destravado pelo metadado, não pelo código: o `Table.Update.latest`
está em `2026-08-05`, anterior à `data_extracao` de qualquer run. Ele passa a ser um
relógio de materialização depois do primeiro run de prod com `update_metadata=True`.

O poll é chamado sem condição e está pinado em `env="prod"`. Um run com
`update_metadata=false` também depende do backend de produção, e falha nessa task se
ele estiver indisponível.

### 5. Onde fica o join do município: no dbt

A fonte dá o **nome** do município, e a convenção exige `id_municipio` com FK
para o diretório.

O join fica no modelo dbt, que é o padrão do repo para join contra produção. O
staging carrega `nome_municipio`; `id_municipio` só existe na tabela final, e o nome
não sobrevive a ela, já que vive no diretório.

Consequências no código:

- `constants.STAGING_SUBSTITUICOES` declara a divergência
  (`id_municipio` → `nome_municipio`), e o `write_partitioned` a aplica ao montar
  a ordem das colunas do parquet a partir da arquitetura;
- a normalização de nome e o mapa de municípios renomeados **não** ficam em
  Python: viram SQL, no formato do modelo do BNDES;
- o `not_null` em `id_municipio` no `schema.yml` é o que impede perda silenciosa,
  porque o `left join` devolve NULL para quem não casar.

A taxa de casamento medida, as seis exceções na forma do `case` e o caso do acento
grave estão no roadmap da task, Etapa 4.

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
quando a página muda. Os XPaths em `constants.XPATHS` são mistos: o painel e o
checkbox de tipologias são ancorados por texto, enquanto o widget de estado, o select
oculto e o botão de export usam os ids `abas:sanfonas:*`. Esses três estão acoplados
ao HTML da fonte e precisam ser revalidados quando a página mudar.

O painel "Reconhecimentos vigentes" oferece PDF, XLS e CSV.

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
`pipelines/crawler/stf_corte_aberta/utils.py` e `pipelines/crawler/bcb/utils.py`,
ambos esperando com `time.sleep` fixo. Aqui a espera é por `WebDriverWait` e por
`_wait_for_download`, que sonda o `.crdownload` até o arquivo fechar.

Restrições que vêm com a escolha:

- `--disable-dev-shm-usage` é obrigatório no k8s, porque o `/dev/shm` do pod é
  pequeno;
- o `job_variables = {"memory": "4Gi"}` em `flows.py` não foi medido;
- a página tem **vários** botões "Exportar CSV", um por relatório. O seletor precisa
  ser relativo ao painel certo, senão baixa o relatório errado, sem erro.

## Estrutura

```text
pipelines/datasets/br_sedec_desastres/
├── constants.py   URLs, XPaths, timeouts, COLUNAS (o schema)
├── utils.py       download + limpeza (funções puras, sem Prefect)
├── tasks.py       @task envolvendo utils (é onde ficam os retries)
├── flows.py       o @flow: ordem das etapas + schedule
└── README.md      este arquivo

models/br_sedec_desastres/
├── br_sedec_desastres__reconhecimentos_vigentes.sql modelo dbt
└── schema.yml                                       testes
```

Uma base brasileira não tem `code/` sob `models/`: dos 158 diretórios em `models/`,
18 têm `code/architecture/`, e nenhum deles é base de dados brasileira — dois são
diretórios de outros países. Arquitetura commitada, script de limpeza local e
`upload.py` são o padrão de onboarding **internacional**.

Em consequência:

- **a planilha de arquitetura fica em `task_davi/bndes/`**, em **xlsx** (o padrão),
  fora do repo, com uma cópia como Planilha Google no Drive, exigida pelo
  `upload_columns_from_sheet` (ver Metadados). Como o código não pode lê-la, o schema
  (ordem, tipos, `original_name`) vive em `constants.COLUNAS`, e toda alteração na
  planilha precisa ser refletida lá;
- **não há carga inicial separada.** A primeira carga é o `run_local.py` deste
  diretório rodando `download,clean,upload`, promovida por PR;
- as etapas são executáveis localmente pelo `run_local.py`: ele chama as mesmas
  `@task` do flow via `.fn()` e escreve em `tmp/br_sedec_desastres/`. Ele é
  versionado porque **é** o mecanismo de atualização da base (ver "Atualização
  mensal" abaixo), não um rascunho. O que ele não cobre é a fiação do `flows.py`
  — que hoje não roda em lugar nenhum.

## Metadados

Registrados em prod, com o dataset em `status = under_review`, o que o mantém
invisível no site até a publicação — passo pós-merge.

Registros criados: a organização `sedec`, o dataset de slug `desastres` sob ela, uma
raw data source apontando para `https://s2id.mi.gov.br/paginas/relatorios/`, a tabela
`reconhecimentos_vigentes` com as oito colunas, e três observation levels — `day`,
`municipality` e `disaster`. Os ids ficam fora deste arquivo; obtê-los é
`get_dataset("desastres", env="prod")`.

Os três observation levels estão ligados às colunas do grão: `data_extracao`,
`id_municipio` e `id_cobrade`. Sem esse vínculo o site exibe "Não informado".

Cobertura: área `br`, uma Coverage com `is_closed=False` e DateTimeRange
`2026-08-06 → 2026-08-06`. O fim não fica aberto porque `end_year` é obrigatório na
API; o `register_table_materialization_task` reescreve a faixa a cada run.

A tabela tem uma única raw source. Tabela com duas fontes ligadas quebra o poll,
porque `_raw_source_id` levanta erro quando a query casa mais de um nó. A raw source
do portal de dados abertos do MDR (`dadosabertos.mdr.gov.br`) permanece ligada ao
dataset `s2id`.

### Limitações do registro por MCP

O `bigqueryType` das colunas só é escrito pelo `upload_columns_from_sheet`. Nem
`bulk_upsert_columns` nem `update_column` têm campo de tipo, e o primeiro também não
escreve `temporal_coverage` nem `is_partition`. Como o `upload_columns_from_sheet` lê
a planilha do Google exportada em CSV, a arquitetura precisa estar no Drive:
arquitetura apenas em xlsx local não permite tipar as colunas.

A planilha alimenta somente o `descriptionPt`. As descrições em inglês e espanhol
são escritas num segundo call, de `bulk_upsert_columns`. Reexecutar o upload da
planilha sobrescreve o PT e mantém EN e ES no valor anterior.

Nenhuma ferramenta de leitura devolve a descrição de uma coluna: o `get_dataset` traz
apenas id e nome. Conferir descrição de coluna exige o Django admin.

## Atualização mensal — manual, promovida por PR

Decidido em 2026-08-10, com a supervisão. O S2ID barra o IP de saída do cluster
por geolocalização: a raspagem não roda no pod, e não há correção possível no
código do pipeline. O `flows.py` continua no repo, sem schedule, para o caso de a
rede ser liberada; quem atualiza a base é o `run_local.py`, e a promoção para prod
é a action `table-approve`.

**O prefixo `staging/br_sedec_desastres/reconhecimentos_vigentes/` no bucket
`basedosdados-dev` é o histórico da série.** No merge, o `push_table_to_bq` do
`prefect_run_dbt.py` espelha esse prefixo para o bucket `basedosdados` — apaga o
que havia lá (com backup em `basedosdados-backup`) e copia o que está em dev. Ou
seja: apagar do prefixo de dev apaga o dado de prod no merge seguinte, e o que
estiver nele na hora do merge é exatamente o que vai ao ar. O `tmp/` local, esse
pode limpar à vontade.

1. `uv run python pipelines/datasets/br_sedec_desastres/run_local.py --stages download,clean,upload`
2. Bumpar o `-- Último retrato promovido:` no topo do `.sql`. Sem `.sql` alterado
   a action não age: o `prefect_run_dbt.py` monta a lista de tabelas a partir dos
   arquivos `.sql` modificados na PR.
3. Abrir a PR com a label `table-approve` e mergear.
4. Conferir a materialização em `basedosdados.br_sedec_desastres.reconhecimentos_vigentes`.
5. `uv run python pipelines/datasets/br_sedec_desastres/run_local.py --stages metadata`
   — depois do merge, porque essa etapa lê a data máxima da tabela de prod.

A etapa `metadata` precisa de Python 3.11: `pipelines/utils/metadata/domain.py`
usa `enum.StrEnum`. Em venv 3.10 o import falha (`uv venv --python 3.11`).

**Retrato não gerado é retrato perdido.** A fonte publica apenas o estado atual,
e a vigência é de 180 dias: um mês sem rodar abre um buraco irrecuperável na
série.

## O que falta

- [ ] Primeira promoção: PR com a label `table-approve`, merge, e a materialização
      conferida em prod.
- [ ] Pós-merge: rodar a etapa `metadata` e mudar o dataset para `published`.
