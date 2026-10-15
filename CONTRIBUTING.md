<p align="center">
    <a href="https://basedosdados.org">
        <img src="https://storage.googleapis.com/basedosdados-website/logos/bd_minilogo.png" width="340" alt="Base dos Dados">
    </a>
</p>

<p align="center">
    <em>Universalizando o acesso a dados de qualidade.</em>
</p>

# Contribuindo

Neste documento, mostra-se como configurar o ambiente para executar os 2 processos de dados existentes na BD: Elaboração de pipelines; Elaboração de códigos de ELT/ETL para subida de dados com frequência de atualização baixa.

Este guia é dedicado para novos integrantes da equipe da BD e voluntários que desejam colaborar com o projeto.

## Configuração de ambiente para desenvolvimento

### Requisitos

> [!WARNING]
> Você precisa ter uma conta no [GitHub](https://github.com/) e ter o `git` configurado.

- [git](https://git-scm.com/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Um editor de texto (recomendado [VS Code](https://code.visualstudio.com/))
- WSL 2, apenas para usuário Windows
  - Se você usa o Windows é essencial Instalar o WSL 2 (Ubuntu). Siga esse [passo a passo](https://learn.microsoft.com/pt-br/windows/wsl/install)

Clone esse repositório

```sh
git clone git@github.com:basedosdados/pipelines.git
```

Entre no repositório clonado

```sh
cd pipelines
```

#### Instalar as dependências

```sh
uv sync
```

Instalar os hooks de pré-commit (ver https://pre-commit.com/ para entendimento dos hooks)

```sh
uv run pre-commit install --install-hooks
```

Instale as dependências do `dbt`

```sh
uv run dbt deps
```

#### Configurar as credenciais do DBT

Crie um variável ambiente `BD_SERVICE_ACCOUNT_DEV` apontado para o arquivo da conta de serviço.

Abra o arquivo `~/.bashrc` com seu editor ou use `nano ~/.bashrc` e adicione no final do arquivo.

> [!NOTE]
> Certifique-se de o arquivo JSON existir. No exemplo abaixo ele está em `$HOME/.basedosdados/credentials/staging.json`

```sh
export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/staging.json"
```

Salve, feche e execute `exec bash`

### Erros comuns

- Se atente para sempre carregar o arquivo `.env` com o comando `source .env`
  - Há a extensão do vscode chamada Python Environment Manager que você consegue ver e configurar as envs. Segue o link: [Python Environment Manager](https://marketplace.visualstudio.com/items?itemName=donjayamanne.python-environment-manager)
  - Garanta que o arquivo `.env` está certinho:
  - Não deve ter espaços após o `:`
  - Não pode ter `_` a mais nem a menos
- Não se esqueça de criar o arquivo `auth.toml` na pasta `$HOME/.prefect` conforme descrito no `README.md`
  - Caso você não tenha a api_key do arquivo auth.toml, mande mensagem para a Laura, uma vez que é uma chave pessoal.

## Pipelines

Essa seção cobre o desenvolvimento de pipelines. Pipelines são construídas usando o prefect e são para dados com frequência de atualização alta.

### Estrutura de diretórios

```
datasets/                    # diretório raiz para o órgão
|-- projeto1/                # diretório de projeto
|-- |-- __init__.py          # vazio
|-- |-- constants.py         # valores constantes para o projeto
|-- |-- flows.py             # declaração dos flows
|-- |-- schedules.py         # declaração dos schedules
|-- |-- tasks.py             # declaração das tasks
|-- |-- utils.py             # funções auxiliares para o projeto
...
|-- __init__.py              # importa todos os flows de todos os projetos
|-- constants.py             # valores constantes para o órgão
|-- flows.py                 # declaração de flows genéricos do órgão
|-- schedules.py             # declaração de schedules genéricos do órgão
|-- tasks.py                 # declaração de tasks genéricas do órgão
|-- utils.py                 # funções auxiliares para o órgão

...

utils/
|-- __init__.py
|-- flow1/
|-- |-- __init__.py
|-- |-- flows.py
|-- |-- tasks.py
|-- |-- utils.py
|-- flows.py                 # declaração de flows genéricos
|-- tasks.py                 # declaração de tasks genéricas
|-- utils.py                 # funções auxiliares

constants.py                 # valores constantes para todos os órgãos

```

### Adicionando órgãos e projetos

O script `manage.py` é responsável por criar e listar pipelines desse repositório.

Você pode obter mais informações sobre os comandos com

```sh
uv run manage.py --help
```

O comando `add-pipeline` permite que você adicione uma nova pipeline a partir do template padrão. Para fazê-lo, basta executar

```sh
uv run manage.py add-pipeline nome_da_pipeline
```

Isso irá criar um diretório com o nome da pipeline em `pipelines/datasets/` com o template padrão, já adaptado ao nome do órgão. O nome do órgão deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os órgãos existentes e nomes reservados

```sh
uv run manage.py list-pipelines
```

### Testar uma pipeline localmente

> [!NOTE]
> As funções `run_local`/`run_cloud` citadas em versões antigas deste guia não existem mais — eram do Prefect 1. O jeito atual (Prefect 3) é o que segue.

O Prefect 3 sempre precisa de algum backend (servidor) pra rodar qualquer `@task`/`@flow`, mesmo fora de um deployment — não dá pra chamar a função "pura", sem mais nada, como no Prefect 1. `scripts/dev_local.sh` sobe esse backend localmente (servidor + work pool + worker, tudo rodando na sua própria máquina, sem precisar de Docker nem de nada além do que o `uv sync` já instalou), deploya sozinho os flows genéricos compartilhados (`build_and_promote`, `run_dbt_model`, transferência pra prod — sempre, mesmo sem mudança neles, já que qualquer dataset migrado depende deles) e os flows dos `.py` alterados em relação a `origin/main` (commitados ou não — inclusive arquivo novo não rastreado):

```sh
set -a && source .env && set +a   # credenciais do Vault — ver nota abaixo
scripts/dev_local.sh up
export PREFECT_API_URL="http://127.0.0.1:4200/api"   # a própria saída do comando já mostra essa linha
```

> [!IMPORTANT]
> **`check_update` precisa das credenciais do Vault mesmo rodando local** — ele registra o poll no backend (`poll_source_for_update_task`/`commit_source_update_task`), que autentica via `get_credentials_from_secret`/`VAULT_ADDRESS`/`VAULT_TOKEN`. Sem isso no ambiente, quebra com `AttributeError: 'NoneType' object has no attribute 'strip'`. Sourcear o `.env` do repositório (que já tem essas duas variáveis, ver `.env.example`) antes do `up` resolve — o worker herda o ambiente de quem o iniciou.

A UI fica em `http://127.0.0.1:4200`. Se você não mudou nada ainda (ex. acabou de trocar de branch), o `up` não deploya nenhum dataset sozinho — mexa no dataset primeiro, ou force tudo: `scripts/dev_local.sh up --all` (deploya os ~330 flows do repositório, mais lento). Se 1-2 flows falharem no deploy com `503` (SQLite local sob concorrência, não um erro do flow em si), o script avisa qual foi e você redeploya só aquele.

**Flow monolítico (ainda não migrado pro pipeline em estágios)**: o `@flow` já é uma função Python chamável direto. A maioria aceita `materialize_after_dump=False`/`force_run=True` (ou nomes parecidos) — isso já garante que só escreve em dev, nunca em prod:

```python
from pipelines.datasets.br_fgv_igp.flows import br_fgv_igp__igp_di_mes

br_fgv_igp__igp_di_mes(materialize_after_dump=False, force_run=True)
```

**Pipeline em estágios (`check_update`/`extract_and_load`, ver `pipelines/utils/stage_dispatch.py`)**: se você editou esse dataset antes do `up`, o deploy já aconteceu sozinho — é só disparar o run:

```sh
uv run prefect deployment run 'check_update: meu_dataset__minha_tabela/dev-check_update'
```

Ou pela própria UI, na página do deployment. Isso dispara a cadeia `check_update → extract_and_load → build_and_promote` automaticamente — igual em produção na lógica (achou dado novo → despacha o próximo estágio), mas rodando tudo localmente e só escrevendo em `basedosdados-dev`.

> [!NOTE]
> **Por que o despacho local não usa `run_deployment()` de verdade**: `run_deployment()` chamado de dentro de um flow já em execução (necessário pra encadear os estágios) quebra com `422 Unprocessable Entity` contra o servidor Prefect local — bug isolado que reproduz igual com qualquer subflow, mesmo sem `run_deployment()`/deployment nenhum envolvido (não é erro no seu dataset). `stage_dispatch.py` detecta o pool local (tag `env:local`) e, só nesse caso, chama a função do próximo estágio direto (`.fn()`, sem passar pelo mecanismo de subflow que está quebrado) em vez de despachar via deployment — o comportamento em dev real/produção não muda. Efeito colateral: localmente a cadeia roda **síncrona** (`check_update` só termina depois que `extract_and_load`/`build_and_promote` terminarem), ao contrário do fire-and-forget de produção — é por isso que, testando localmente, o comando de `deployment run` pode demorar bem mais que só o primeiro estágio.

**Editou o código depois do `up`? Na maioria das vezes não precisa redeployar** — o worker local lê o arquivo do disco a cada execução (nada é empacotado), então mudança de lógica dentro do flow/task já vale no próximo run, sem mais nada (confirmado na prática: editar o corpo de um flow já deployado e rodar de novo, sem redeploy, pega a versão nova). Só precisa rodar isso se a mudança for em **metadado do deployment em si** — schedule, `job_variables`, `deploy_tags`, ou a assinatura de parâmetros do `@flow`:

```sh
uv run .github/workflows/scripts/deploy_flows.py --local --files pipelines/datasets/meu_dataset/flows.py
```

Quando terminar:

```sh
scripts/dev_local.sh down
```

### Testar uma pipeline na nuvem

Faça a cópia do arquivo `.env.example` para um novo arquivo nomeado `.env`:

```sh
cp .env.example .env
```

Substitua os valores das seguintes variáveis pelos seus respectivos valores:

- `GOOGLE_APPLICATION_CREDENTIALS`: Path para um arquivo JSON com as credenciais da API do Google Cloud
  de uma conta de serviço com acesso de escrita ao bucket `basedosdados-dev` no Google Cloud Storage.
- `VAULT_TOKEN`: deve ter o valor do token do órgão para o qual você está desenvolvendo. Caso não saiba o token, entre em contato.

Carregue as variáveis de ambiente do arquivo `.env`:

```sh
source .env
```

Também, garanta que o arquivo `$HOME/.prefect/auth.toml` exista e tenha um conteúdo semelhante ao seguinte:

```toml
["prefect.basedosdados.org"]
api_key = "<sua-api-key>"
tenant_id = "<tenant-id>"
```

O valor da chave `tenant_id` pode ser coletada através da seguinte URL: https://prefect.basedosdados.org/default/api. Devendo ser executado a seguinte query:

```graphql
query {
  tenant {
    id
  }
}
```

Em seguida, tenha certeza que você já tem acesso à UI do Prefect, tanto para realizar a submissão da run, como para acompanhá-la durante o processo de execução.

Crie o arquivo `test.py` com a pipeline que deseja executar e adicione a função `run_cloud` com os parâmetros necessários:

```python
from pipelines.utils.utils import run_cloud
from pipelines.[secretaria].[pipeline].flows import flow # Complete com as infos da sua pipeline

run_cloud(
    flow,               # O flow que você deseja executar
    labels=[
        "example",      # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters = {
        "param": "val", # Parâmetros que serão passados para a pipeline (opcional)
    }
)
```

Execute a pipeline com:

```sh
uv run test.py
```

A saída deverá se assemelhar ao exemplo abaixo:

```log
[2022-02-19 12:22:57-0300] INFO - prefect.GCS | Uploading xxxxxxxx-development/2022-02-19t15-22-57-694759-00-00 to basedosdados-dev
Flow URL: http://localhost:8080/default/flow/xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
└── ID: xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
└── Project: main
└── Labels: []
Run submitted, please check it at:
https://prefect.basedosdados.org/flow-run/xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

(Opcional, mas recomendado) Quando acabar de desenvolver sua pipeline, delete todas as versões da mesma pela UI do Prefect.

## Como subir dados

Essa seção explica como enviar dados para o datalake da BD.

### Primeiros passos

1. Escolher a base e entender mais dos dados
2. Preencher as tabelas de arquitetura
   - As tabelas de arquitetura determinam qual a estrutura de cada tabela do seu conjunto de dados. Elas definem, por exemplo, o nome, ordem e metadados das variáveis, além de compatibilizações quando há mudanças em versões (por exemplo, se uma variável muda de nome de um ano para o outro).
   - [Template da tabela de arquitetura para preencher](https://docs.google.com/spreadsheets/d/1sReQvLG6s53BUcvfoeQOS6SOvXRGdv1Li1qFD9oWMds/edit?gid=1596237098#gid=1596237098)
   - Leia o [Manual de estido da BD](https://basedosdados.org/docs/style_data) para preencher a tabela de arquitetura
   - Compartilhe a tabela com a equipe de dados da BD para aprovar.
3. Escrever código de captura e limpeza de dados
   - Os arquivos devem estar no formato CSV ou Parquet
4. Chave de acesso ao Google Cloud dos voluntários

### Upload dos dados brutos no BigQuery

Você precisa adicionar seu código em `models/<dataset-id>/code/` na pasta do dataset específico.

Para subir os dados, use o módulo da BD.

```python
import basedosdados as bd

DATASET_ID = "dataset_id"  # Nome do dataset
TABLE_ID = "table_id"  # Nome da tabela

tb = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
```

No próximo passo carregamos os dados para o Storage da BD e criamos uma tabela BigQuery que por uma conexão externa acessa esses dados diretamente do Storage

> Deixamos os parâmetros que mais usamos visíveis aqui, mas é importante explorar outras opções de parâmetros na documentação.

```python
tb.create(
    path=path_to_data,  # Caminho para o arquivo csv ou parquet
    if_storage_data_exists="raise",
    if_table_exists="replace",
    source_format="csv",
)
```

### Criar os arquivos do DBT

```python
from databasers_utils import TableArchitecture

arch = TableArchitecture(
    dataset_id="<dataset-id>",
    tables={
        "<table-id>": "URL da arquiterura do Google Sheet",  # Exemplo https://docs.google.com/spreadsheets/d/1K1svie4Gyqe6NnRjBgJbapU5sTsLqXWTQUmTRVIRwQc/edit?usp=drive_link
    },
)

# Cria o yaml file
arch.create_yaml_file()

# Cria os arquivos sql
arch.create_sql_files()

# Atualiza o dbt_project.yml
arch.update_dbt_project()

# Faz o upload das colunas para o DJango
# Para essa etapa é necessário ter duas varíaveis de ambiente configurada
# BD_DJANGO_EMAIL="seuemail@basedosdados.org"
# BD_DJANGO_PASSWORD="password"
arch.upload_columns()
```

#### Macro `set_datalake_project`

Os arquivos sql do dbt usam a macro [`set_datalake_project`](./macros/set_datalake_project.sql) que indica de qual projeto (basedosdados-staging ou basedosdados-dev) serão consumidos os dados. Ao criar os arquivos usando a função `create_sql_files` a macro será inserida.

```sql
select
    col_name
from {{ set_datalake_project("<DATASET_ID>_staging.<TABLE_ID>") }}
```

> [!IMPORTANT]
> Não use a macro para fazer join, joins devem ser feitos com a tabelas na zona de produção usando o projeto basedosdados `basedosdados.<DATASET_ID>.<TABLE_ID>` Exemplo: [modelo br_bd_diretorios_brasil__distrito_2022.sql](https://github.com/basedosdados/pipelines/blob/main/models/br_bd_diretorios_brasil/br_bd_diretorios_brasil__distrito_2022.sql)

## Usando o DBT

> [!IMPORTANT]
> Ative o ambiente virtual (venv) com `source .venv/bin/activate` para executar os comandos `dbt`.

### Materializando o modelo no BigQuery

> [!IMPORTANT]
> No arquivo de configuração do DBT schema.yml o target é pré definido como dev. Com esta configuração, quando um modelo dbt for executado os dados serão consumidos do projeto basedosdados-dev.*_staging. Deste modo, não é preciso informar a flag --target no momento de testagem e validação de modelos em ambientes locais.

Materializa um único modelo pelo nome em basedosdados-dev consumindo os dados de basedosdados-dev.{table_id}_staging

```sh
dbt run --select dataset_id__table_id
```

Materializa todos os modelos em uma pasta em basedosdados-dev consumindo os dados de basedosdados-dev.{table_id}_staging

```sh
dbt run --select model.dateset_id.dateset_id__table_id
```

Materializa todos os modelos no caminho em basedosdados-dev consumindo os dados de basedosdados-dev.{table_id}_staging

```sh
dbt run --select models/dataset_id
```

Materializa um único modelo pelo caminho do arquivo sql em basedosdados-dev consumindo os dados de basedosdados-dev.{table_id}_staging

```sh
dbt run --select models/dataset/table_id.sql
```

### Testes

Os testes do modelo são definidos no arquivo `schema.yml`

#### Tipos de testes

##### Conexão com diretórios

```yaml
---
models:
  - name: orders
    columns:
      - name: id_municipio
        tests:
          - relationships:
              to: ref('br_bd_diretorios_brasil__municipio')
              field: id_municipio
```

##### Chaves únicas

```yaml
---
models:
  - name: orders
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [country_code, order_id]
```

> [!NOTE]
> Os testes abaixo são customizados, eles ficam em [`tests-dbt/generic`](./tests-dbt/generic/)

##### Não nulidade das colunas

```yaml
---
models:
  - name: dataset_id__table_id
    description:
    tests:
      - not_null_proportion_multiple_columns:
          at_least: 0.95
```

##### Conexão com diretórios customizada

Permite explicitar valores que vão ser ignorados no momento de realizar o teste de relação e a proporção de valores sem correspondência que será tolerada.

> [!WARNING]
> Caso utilize esse teste é essencial documentar nos metadados da tabela quais exceções foram aplicadas e porque elas são aceitáveis. De preferência adicionar essas informações na descrição da tabela para que o usuário seja informado dessas exceções

```yaml
---
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_relationships:
        to: ref('br_bd_diretorios_mundo__sistema_harmonizado')
        field: id_sh4
        ignore_values: ['5410']  # O valor 5410 será ignorado
        proportion_allowed_failures: 0
```

##### Chave única customizada

Permite inserir uma proporção de chaves únicas que repetidas que pode ser tolerada. Usar com parcimônia! Pode mascarar repetição de linhas.

> [!WARNING]
> Caso utilize esse teste é essencial documentar nos metadados da tabela quais exceções foram aplicadas e porque elas são aceitáveis. De preferência adicionar essas informações na descrição da tabela para que o usuário seja informado dessas exceções

```yaml
---
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns: [column_1, column_2]
          proportion_allowed_failures: 0.05
```

##### Testes Incrementais

Para tabelas muito grandes é importante que o teste rode apenas nas linhas novas que serão incluidas. Para isso usaremos o config `where` e uma das keywords `__most_recent_year_month__` | `__most_recent_date__` | `__most_recent_year__`

###### `where`

É inserido à nível do teste e permite inserir lógica SQL para filtrar os dados.

```yaml
---
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns: [column_1, column_2]
          proportion_allowed_failures: 0.05
          config:
            where: 'date_column = '2024-01-01''
```

###### `where + keyword`

A macro [`custom_get_where_subquery`](macros/custom_get_where_subquery.sql) detecta a presença de uma das keywords acima e executa uma consulta para determinar os valores mais recentes de ano e mês, data ou ano de uma tabela. Em seguida, substitui a keyword declarada pelos valores mais recentes encontrados, o que garante que o teste seja executado apenas nas linhas mais recentes da tabela.

- `__most_recent_year_month__` : A macro faz um query usando as colunas `ano` e `mes` para identificar a data mais recente
- `__most_recent_date__` : A macro faz um query usando a coluna `data` para identificar a data mais recente
- `__most_recent_year__` : A macro faz um query usando a coluna `ano` para identificar o ano

```yaml
---
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns: [column_1]
          proportion_allowed_failures: 0.05
          config:
            where: __most_recent_year_month__
```

### Executando os testes

Testa um único modelo

```sh
dbt test --select dataset_id__table_id
```

Testa todos os modelos em uma pasta

```sh
dbt test --select model.dateset_id.dateset_id__table_id
```

Testa todos os modelos no caminho

```sh
dbt test --select models/dataset_id
```
