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

> [!WARNING]
> O `dbt_project.yml` define `packages-install-path: /app/dbt_packages`, que é um caminho **de container** e quebra o `deps` localmente. Aplique o workaround do seu engine:
>
> - **dbt-core 1.8** (não aceita override): crie o diretório uma vez —
>   ```sh
>   sudo mkdir -p /app/dbt_packages && sudo chown -R "$USER" /app/dbt_packages
>   ```
> - **dbt Fusion**: exporte o caminho local antes de rodar `dbtf deps` —
>   ```sh
>   export DBT_PACKAGES_INSTALL_PATH="$PWD/dbt_packages"
>   ```

#### (Opcional) Instalar o dbt Fusion engine

Estamos migrando, de forma faseada, do `dbt-core` (Python) para o **dbt Fusion engine** (Rust). O Fusion **não** é instalado pelo `uv sync` — é um binário separado:

```sh
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
dbtf --version
dbtf deps
```

Durante a fase de migração os dois engines convivem:

- `uv run dbt ...` → dbt-core (engine atual de produção)
- `dbtf ...` → dbt Fusion (em validação)

> [!NOTE]
> O `dbtf` é um binário standalone e **não** precisa do venv ativo. Apenas o `dbt` (core) depende do ambiente Python.

> [!WARNING]
> O dbt Fusion engine é licenciado sob **ELv2** (não Apache-2.0 como o dbt-core), e a extensão de VS Code é gratuita por 14 dias / até 15 usuários por organização. O adapter BigQuery do Fusion ainda está em Preview/beta — valide modelos no dev antes de confiar na saída.

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

Escolha a pipeline que deseja executar (exemplo `pipelines.rj_escritorio.test_pipeline.flows.flow`). Crie um arquivo `test.py` na pasta e importe o flow

```python
from pipelines.datasets.test_pipeline.flows import flow
from pipelines.utils.utils import run_local

run_local(flow, parameters={"param": "val"})
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
> Para o **dbt-core**, ative o ambiente virtual (venv) com `source .venv/bin/activate` (ou use `uv run dbt ...`) antes de executar os comandos `dbt`.
> Para o **dbt Fusion** (`dbtf`), o venv não é necessário — é um binário standalone. Os comandos abaixo (`dbt run`, `dbt test`, `--select`) têm a mesma sintaxe nos dois engines; basta trocar `dbt` por `dbtf`.

> [!NOTE]
> Algumas macros e testes customizados usam Jinja sensível ao engine. Ao **alterar qualquer um dos arquivos abaixo**, faça um smoke-test no Fusion antes de abrir o PR:
>
> - `macros/custom_get_where_subquery.sql` — usa `run_query()` em parse-time (maior risco).
> - `macros/set_datalake_project.sql`, `macros/dicionario_not_found.sql` — usam `exceptions.raise_compiler_error()`.
> - `macros/generate_schema_name.sql` — sobrescreve uma macro built-in.
> - `tests-dbt/generic/not_null_proportion_multiple_columns.sql` — usa `adapter.get_columns_in_relation()` + `dbt_utils.get_query_results_as_dict()`.
> - `pipelines/utils/gcs.py` (`DBTArtifactUploader`) — lê `target/{compiled,run}`; confirme que o Fusion gera o mesmo layout.
>
> Comandos de validação:
>
> ```sh
> # O Fusion exige o formato novo de args de teste (`arguments:`); o repo mantém
> # o formato antigo p/ o dbt-core. Aplique a conversão efêmera antes de validar
> # (reverta depois com `git checkout -- models dbt_project.yml`):
> uvx --python 3.12 dbt-autofix deprecations
> dbtf deps
> dbtf parse
> dbtf compile --select <modelo> 2>&1 | tee /tmp/fusion_compile.log
> ```

### Selecionando o engine (`DBT_ENGINE`)

Em produção o engine é escolhido em runtime, sem alteração de código. A chave mora em [`pipelines/utils/execute_dbt_model/engine.py`](./pipelines/utils/execute_dbt_model/engine.py):

| Variável | Valores | Default | Função |
|----------|---------|---------|--------|
| `DBT_ENGINE` | `core` \| `fusion` | `core` | `core` usa o `dbtRunner` (dbt-core); `fusion` invoca o binário `dbtf` via subprocess |
| `DBT_FUSION_BIN` | nome/caminho | `dbtf` | binário do Fusion a ser chamado |
| `DBT_FUSION_VERSION` | versão | *(vazio = latest)* | ARG de build do Docker que fixa a versão do Fusion na imagem |

No worker do Prefect, defina `DBT_ENGINE` de uma destas formas:

- via **Base Job Template** do work pool (UI do Prefect → pool → `env`); ou
- por deployment: `job_variables={"env": {"DBT_ENGINE": "fusion"}}`.

> [!NOTE]
> **Rollback é instantâneo e sem mudança de código:** remova `DBT_ENGINE` (ou defina `=core`). O default já é `core`, então o dbt-core (engine de produção) permanece o fallback.

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

> [!IMPORTANT]
> O **dbt Fusion** exige que os argumentos de testes genéricos fiquem aninhados sob a chave `arguments:`. O formato antigo (args no topo do teste), usado nos exemplos abaixo, é aceito pelo dbt-core 1.8.x (engine de produção) mas rejeitado pelo Fusion. O formato novo, por sua vez, só é entendido pelo dbt-core ≥ 1.10 — que exige `protobuf ≥ 5`, incompatível com as libs Google fixadas no projeto (`google-analytics-data`, `google-api-core`, `googleapis-common-protos` exigem `protobuf < 5`).
>
> Por isso o repositório **mantém o formato antigo** nos `schema.yml` committados (para o dbt-core continuar funcionando), e a conversão para `arguments:` é aplicada **em tempo de build, só para o Fusion** — de forma efêmera, via `uvx --python 3.12 dbt-autofix deprecations` (job `fusion-checks` do CI; ver [`.github/workflows/cd-staging.yaml`](./.github/workflows/cd-staging.yaml)). A transformação **não** é commitada. Exemplo antes/depois:
>
> ```yaml
> # committado no repo (dbt-core 1.8.x)
> - dbt_utils.unique_combination_of_columns:
>     combination_of_columns: [country_code, order_id]
> # gerado pelo autofix em build-time (exigido pelo Fusion)
> - dbt_utils.unique_combination_of_columns:
>     arguments:
>       combination_of_columns: [country_code, order_id]
> ```

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
