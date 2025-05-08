<p align="center">
    <a href="https://basedosdados.org">
        <img src="https://github.com/basedosdados/sdk/blob/master/docs/docs/pt/images/bd_minilogo.png" width="340" alt="Base dos Dados">
    </a>
</p>

<p align="center">
    <em>Universalizando o acesso a dados de qualidade.</em>
</p>

# Contribuindo

Neste documento, mostra-se como configurar o ambiente e desenvolver novas features para as pipelines da **BD**. O tutorial é dedicado a não-membros da **BD** e, assim, cobre apenas o caso de desenvolvimento local. Futuramente, o desenvolvimento em cloud estará disponível também para não-membros.

## Configuração de ambiente para desenvolvimento

### Requisitos

> [!WARNING]
> Você precisa ter uma conta no [GitHub](https://github.com/) e ter o `git` configurado.

- Um editor de texto (recomendado [VS Code](https://code.visualstudio.com/))
- WSL 2, apenas para usuário Windows
- [`git`](https://git-scm.com/)
- [`pyenv`](https://github.com/pyenv/pyenv): Para gerenciar versões do `python`
- [`poetry`](https://python-poetry.org/): Para gerenciar as dependências

Clone esse repositório

```sh
git clone git@github.com:basedosdados/pipelines.git
```

Entre no repositório clonado

```sh
cd pipelines
```

#### Instalar o WSL 2 (Ubuntu) - (Apenas usuário Windows)

Se você usa o Windows é essencial Instalar o WSL 2 (Ubuntu). Siga esse [passo a passo](https://learn.microsoft.com/pt-br/windows/wsl/install)

#### Instalar o `pyenv`

É importante instalar o `pyenv` para garantir que a versão de python é padrão. Escrevemos uma versão resumida, mas recomendamos [esse material](https://realpython.com/intro-to-pyenv/) e [esse](https://gist.github.com/luzfcb/ef29561ff81e81e348ab7d6824e14404) para mais informações.

Instale as dependências:

```sh
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl
```

Instale o `pyenv`

```sh
curl https://pyenv.run | bash
```

> [!IMPORTANT]
> Leia atentamente os avisos depois desse comando, existe um passo a passo essencial para que o `pyenv` funcione

Instale o `python`

> [!NOTE]
> `pyenv` vai instalar a versão em [`.python-version`](/.python-version)

```sh
pyenv install
```

#### Instale o poetry

```sh
curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.8.5 python3 -
```

Configure o poetry para criar o ambiente virtual (venv) no projeto

```sh
poetry config virtualenvs.in-project true
```

Crie o ambiente virtual e/ou ative se já existir

```sh
poetry shell
```

> [!TIP]
> Existe duas forma de ativar um ambiente virtual (venv). Usando `source .venv/bin/activate` (Linux/MacOS) ou `poetry shell`.
> Para desativar o ambiente virtual comando `deactivate`.

#### Instalar as dependências

```sh
poetry install --with dev --with test --no-root
```

> [!WARNING]
> Caso a instalação do `poetry` de erro no pacote do `R`, recomendado rodar a seguinte linha para instalar o R-base `sudo apt -y install r-base`

Instalar os hooks de pré-commit (ver https://pre-commit.com/ para entendimento dos hooks)

```sh
pre-commit install --install-hooks
```

Instale as dependências do `dbt`

```sh
dbt deps
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

> [!IMPORTANT]
> Se você for escrever um pipeline crie um ramo com prefixo `staging/`. Crie usando `git switch -c staging/nome-do-ramo`

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

O script `manage.py` é responsável por criar e listar projetos desse repositório.

Você pode obter mais informações sobre os comandos com

```sh
poetry run python manage.py --help
```

O comando `add-agency` permite que você adicione um novo órgão a partir do template padrão. Para fazê-lo, basta executar

```sh
poetry run python manage.py add-agency nome-do-orgao
```

Isso irá criar um diretório com o nome `nome-do-orgao` em `pipelines/` com o template padrão, já adaptado ao nome do órgão. O nome do órgão deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os órgãos existentes e nomes reservados

```sh
poetry run python manage.py list-projects
```

Em seguida, leia com atenção os comentários em cada um dos arquivos do seu projeto, de modo a evitar conflitos e erros.
Links para a documentação do Prefect também encontram-se nos comentários.

Caso o órgão para o qual você desenvolverá um projeto já exista

```sh
poetry run python manage.py add-project datasets nome-do-projeto
```

### Testar uma pipeline localmente

Escolha a pipeline que deseja executar (exemplo `pipelines.rj_escritorio.test_pipeline.flows.flow`). Crie um arquivo `test.py` na pasta e importe o flow

```python
from pipelines.utils.utils import run_local
from pipelines.datasets.test_pipeline.flows import flow

run_local(flow, parameters = {"param": "val"})
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
poetry run python test.py
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

DATASET_ID = "dataset_id" # Nome do dataset
TABLE_ID = "table_id" # Nome da tabela

tb = bd.Table(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID
)
```

No próximo passo carregamos os dados para o Storage da BD e criamos uma tabela BigQuery que por uma conexão externa acessa esses dados diretamente do Storage

> Deixamos os parâmetros que mais usamos visíveis aqui, mas é importante explorar outras opções de parâmetros na documentação.

```python
tb.create(
    path=path_to_data, # Caminho para o arquivo csv ou parquet
    if_storage_data_exists='raise',
    if_table_exists='replace',
    source_format='csv'
)
```

### Criar os arquivos do DBT

```python
from databasers_utils import TableArchitecture

arch = TableArchitecture(
    dataset_id="<dataset-id>",
    tables={
        "<table-id>": "URL da arquiterura do Google Sheet", # Exemplo https://docs.google.com/spreadsheets/d/1K1svie4Gyqe6NnRjBgJbapU5sTsLqXWTQUmTRVIRwQc/edit?usp=drive_link
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

## Usando o DBT

> [!IMPORTANT]
> Ative o ambiente virtual (venv) com `source .venv/bin/activate` para executar os comandos `dbt`.

### Materializando o modelo no BigQuery

Materializa um único modelo pelo nome

```sh
dbt run --select dataset_id__table_id
```

Materializa todos os modelos em uma pasta

```sh
dbt run --select model.dateset_id.dateset_id__table_id
```

Materializa todos os modelos no caminho

```sh
dbt run --select models/dataset_id
```

Materializa um único modelo pelo caminho do arquivo sql

```sh
dbt run --select models/dataset/table_id.sql
```

### Testes

Os testes do modelo são definidos no arquivo `schema.yml`

#### Tipos de testes

##### Conexão com diretórios

```yaml
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
models:
  - name: orders
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - country_code
            - order_id
```

> [!NOTE]
> Os testes abaixo são customizados, eles ficam em [`tests-dbt/generic`](./tests-dbt/generic/)

##### Não nulidade das colunas

```yaml
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
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_relationships:
        to: ref('br_bd_diretorios_mundo__sistema_harmonizado')
        field: id_sh4
        ignore_values: ["5410"] # O valor 5410 será ignorado
        proportion_allowed_failures: 0
```

##### Chave única customizada

Permite inserir uma proporção de chaves únicas que repetidas que pode ser tolerada. Usar com parcimônia! Pode mascarar repetição de linhas.

> [!WARNING]
> Caso utilize esse teste é essencial documentar nos metadados da tabela quais exceções foram aplicadas e porque elas são aceitáveis. De preferência adicionar essas informações na descrição da tabela para que o usuário seja informado dessas exceções

```yaml
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns:
            - column_1
            - column_2
          proportion_allowed_failures: 0.05
```

##### Testes Incrementais

Para tabelas muito grandes é importante que o teste rode apenas nas linhas novas que serão incluidas. Para isso usaremos o config `where` e uma das keywords `__most_recent_year_month__` | `__most_recent_date__` | `__most_recent_year__`

###### `where`

É inserido à nível do teste e permite inserir lógica SQL para filtrar os dados.

```yaml
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns:
            - column_1
            - column_2
          proportion_allowed_failures: 0.05
          config:
                where: "date_column = '2024-01-01'"
```

###### `where + keyword`

A macro [`custom_get_where_subquery`](macros/custom_get_where_subquery.sql) detecta a presença de uma das keywords acima e executa uma consulta para determinar os valores mais recentes de ano e mês, data ou ano de uma tabela. Em seguida, substitui a keyword declarada pelos valores mais recentes encontrados, o que garante que o teste seja executado apenas nas linhas mais recentes da tabela.

- `__most_recent_year_month__` : A macro faz um query usando as colunas `ano` e `mes` para identificar a data mais recente
- `__most_recent_date__` : A macro faz um query usando a coluna `data` para identificar a data mais recente
- `__most_recent_year__` : A macro faz um query usando a coluna `ano` para identificar o ano

```yaml
models:
  - name: dataset_id__table_id
    description: Table description
    tests:
      - custom_unique_combinations_of_columns:
          combination_of_columns:
            - column_1
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
