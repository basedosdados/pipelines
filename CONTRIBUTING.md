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

> [!NOTE]
> Esse guia é para contribuir com construção de pipelines. Pipelines são para dados com frequência de atualização alta.

## Configuração de ambiente para desenvolvimento

### Requisitos

-   Um editor de texto (recomendado VS Code)
-   WSL 2, apenas para usuários Windows
-   [`git`](https://git-scm.com/)
-   [`pyenv`](https://github.com/pyenv/pyenv): Para gerenciar versões do `python`
-   [`poetry`](https://python-poetry.org/): Para gerenciar as dependências

Clonar esse repositório

```sh
git clone https://github.com/basedosdados/pipelines
```

Entre na repositório clonado

```sh
cd pipelines
```

Crie um ramificação com prefixo `staging`

```sh
git switch -c staging/something
```

### Instalar o WSL 2 (Ubuntu) - (Apenas usuários Windows)

Se você usa o windows é essencial Instalar o WSL 2 (Ubuntu). Siga esse [passo a passo](https://learn.microsoft.com/pt-br/windows/wsl/install)

### Instalar o `pyenv`

É importante instalar o `pyenv` para garantir que a versão de python é padrão. Escrevemos uma versão resumida mas recomendamos [esse material](https://realpython.com/intro-to-pyenv/) e [esse](https://gist.github.com/luzfcb/ef29561ff81e81e348ab7d6824e14404) para mais informações.

Instale as dependencias:

```sh
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl
```

Instale o `pyenv`

```sh
curl https://pyenv.run | bash
```

> [!IMPORTANT]
> Leia atentamente os avisos depois desse comando, existe um passo a passo essencial para que o `pyenv` funcione

Instale a versão 3.10 do `python`

```sh
pyenv install -v 3.10
```

Definir essa versão como versão global

```sh
pyenv global 3.10
```

### Instale o poetry

```sh
curl -sSL https://install.python-poetry.org | python3 -
```

Crie o ambiente virtual ou ative se já existir

```sh
poetry shell
```

#### Instalar as dependências

```sh
poetry install --with dev --no-root
```

Instalar os hooks de pré-commit (ver https://pre-commit.com/ para entendimento dos hooks)

```sh
pre-commit install
```

> [!TIP]
> Caso a instalação do `poetry` de erro no pacote do `R`, recomendado rodar a seguinte linha para instalar o R-base `sudo apt -y install r-base`

### Erros comuns

-   Se atente para sempre carregar o arquivo `.env` com o comando `source .env`
    -   Há a extensão do vscode chamada Python Environment Manager que você consegue ver e configurar as envs. Segue o link: [Python Environment Manager](https://marketplace.visualstudio.com/items?itemName=donjayamanne.python-environment-manager)
    -   Garanta que o arquivo `.env` está certinho:
    -   Não deve ter espaços após o `:`
    -   Não pode ter `_`a mais nem a menos
-   Não se esqueça de criar o arquivo `auth.toml` na pasta `$HOME/.prefect` conforme descrito no `README.md`
    -   Caso você não tenha a api_key do arquivo auth.toml, mande mensagem para a Laura, uma vez que é uma chave pessoal.

## Pipelines

Essa seção cobre o desenvolvimento de pipelines

### Estrutura de diretorios

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
python manage.py --help
```

O comando `add-agency` permite que você adicione um novo órgão a partir do template padrão. Para fazê-lo, basta executar

```sh
python manage.py add-agency nome-do-orgao
```

Isso irá criar um novo diretório com o nome `nome-do-orgao` em `pipelines/` com o template padrão, já adaptado ao nome do órgão. O nome do órgão deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os órgão existentes e nomes reservados

```sh
python manage.py list-projects
```

Em seguida, leia com anteção os comentários em cada um dos arquivos do seu projeto, de modo a evitar conflitos e erros.
Links para a documentação do Prefect também encontram-se nos comentários.

Caso o órgão para o qual você desenvolverá um projeto já exista

```sh
python manage.py add-project datasets nome-do-projeto
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

-   `GOOGLE_APPLICATION_CREDENTIALS`: Path para um arquivo JSON com as credenciais da API do Google Cloud
    de uma conta de serviço com acesso de escrita ao bucket `basedosdados-dev` no Google Cloud Storage.
-   `VAULT_TOKEN`: deve ter o valor do token do órgão para o qual você está desenvolvendo. Caso não saiba o token, entre em contato.

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

O valor da chave `tenant_id` pode ser coletada atráves da seguinte URL: https://prefect.basedosdados.org/default/api. Devendo ser executado a seguinte query:

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
python test.py
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
