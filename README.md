# Pipelines

Esse repositório contém flows desenvolvidos com Prefect relacionados à Base dos Dados.

----

## Criando uma nova pipeline

1. Criar uma pasta com o nome da pipeline na pasta [pipelines/basedosdados/](/pipelines/basedosdados/), caso seja a captura de uma base a pasta deve ser o nome do `dataset_id`. 

2. Na pasta da nova pipeline, devem ser criados os arquivos `flows.py`, `tasks.py`, `schedules.py` e `__init__.py`, como no exemplo da pipeline [pipelines/basedosdados/template_pipeline/](/pipelines/basedosdados/template_pipeline/).

3. Os `flows` da nova pipeline deve ser importado no arquivo [pipelines/basedosdados/__init__.py](/pipelines/basedosdados/__init__.py) 

## Como rodar uma pipeline localmente

 Escolha a pipeline que deseja executar (exemplo `pipelines.emd.test_flow.flows.flow`)

```py
from pipelines.emd.utils import run_local
from pipelines.emd.test_pipeline.flows import flow

run_local(flow, parameters = {"param": "val"})
```

----

## Como acessar a UI do prefect

Requisitos
Conta no GitHub
Ser membro da organização prefeitura-rio no GitHub
Preparo inicial
Primeiramente, deve-se ingressar no Tailscale usando sua conta do GitHub. Para isso, acesse https://login.tailscale.com/ e clique em “Sign in with GitHub”

Em seguida, autorize o acesso solicitado.

Depois, você deve escolher qual “Tailnet” utilizar. Nesse caso, escolha a “basedosdados”:

Dessa forma, você terá permissão para se conectar à VPN.

Nessa página você encontrará instruções de instalação do Tailscale para as diversas plataformas suportadas (macOS, iOS, Windows, Linux e Android). Por serem muitas, as instruções individuais para elas não estão compreendidas nesse documento.
Conectando-se
Como dito anteriormente, não serão explicitados os métodos para todas as plataformas suportadas. Caso haja dúvida, é possível recorrer ao site do Tailscale ou solicitar ajuda à equipe do Escritório Municipal de Dados. Assim, então, os próximos passos considerarão um sistema Linux para uso.

O comando que deve ser executado para acesso pleno à VPN é

```
sudo tailscale up --accept-routes --accept-dns
```

Caso seja solicitado o acesso a um link para autenticação, favor fazê-lo. Assim que o acesso for autorizado, será possível acessar a interface web do Prefect no endereço http://prefect-ui.prefect.svc.cluster.local:8080/.

----
## Como desenvolver

O script `manage.py` é responsável por criar e listar projetos desse repositório. Para usá-lo, no entanto, você deve instalar as dependências em `requirements-cli.txt`. Você pode obter mais informações sobre os comandos

```
python manage.py --help
```

O comando `add-project` permite que você crie um novo projeto a partir do template padrão. Para criar um novo projeto, basta fazer

```
python manage.py add-project nome-do-projeto
```

Isso irá criar um novo diretório com o nome `nome-do-projeto` em `pipelines/` com o template padrão, já adaptado ao nome do projeto. O nome do projeto deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os projetos existentes e nomes reservados, basta fazer

```
python manage.py list-projects
```

Em seguida, leia com anteção os comentários em cada um dos arquivos do seu projeto, de modo a evitar conflitos e erros.
Links para a documentação do Prefect também encontram-se nos comentários.

----
### Requisitos

- Requisitos Python em tempo de execução devem ser adicionados ao `pyproject.toml` na raiz desse repositório.
- Requisitos do `manage.py` estão em `requirements-cli.txt`
- Requisitos para a Action de deployment estão em `requirements-deploy.txt`
- Requisitos para testes estão em `requirements-tests.txt`