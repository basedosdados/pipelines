# Como deletar Flows usando Python

É comum ter que deletar flows em produção ou em desenvolvimento depois de uma alteração específica do Flow ou depois uma alteração geral em todas as pipelines. É possível deletar esses flows na própria UI do Prefect clicando no ícone de lixeira no canto superior direito da página do Flow. O problema dessa solução, contudo, é que quando você precisa deletar muitos flows, você irá gastar muito tempo deletando um por um. Felizmente, é possível usar o cliente em python do prefect para deletar vários flows de uma vez.

Para deletar apenas um flow via python, use o seguinte snippet:

```python
from prefect.client import Client

flow_id = "{flow_hash}"

client = Client()
client.graphql(
    """
    mutation {
    delete_flow(input: {flow_id: "%s"}) {
        success
    }
    }
    """ % flow_id
)
```

Em que a variável `flow_id` é o id do flow que você deseja deletar. Esse id é criado no momento de registro do flow e também pode ser consultado na UI do prefect.

Para deltar todas as versões de um dado flow, basta alterar o argumento no método `client.graphql`:

```python
flow_group_id = "{flow_hash}"

client = Client()
client.graphql(
    """
    mutation {
    delete_flow_group(input: {flow_group_id: "%s"}) {
        success
    }
    }
```

Dessa vez, porém, deve-se usar o id do grupo do Flow de interesse, também disponível na UI.

### Deletando todos os flows via Python

É fácil notar que é possível deletar todos os flows em produção se soubermos de antemão o valor dos ids dos grupos. Uma forma de obter esses ids é fazer um web scraping simples na UI do Prefect e então deletar os flows, como no snippet abaixo:

```python
from prefect.client import Client
from selenium import webdriver
from selenium.webdriver.common.by import By

options = webdriver.ChromeOptions()
options.headless = False
options.add_experimental_option("prefs", {
  "download": {"prompt_for_download": False} })
options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=options)
url = 'http://prefect-ui.prefect.svc.cluster.local:8080/default?flows'
driver.get(url)
elements = driver.find_elements_by_class_name("link")

flow_group_ids=[]
for element in elements:
    link = element.get_attribute('href')
    print(link)
    if link.__contains__('flow'):
        flow_group_ids.append(link.split('/')[-1])


for flow_group_id in flow_group_ids:
    print(f'delete {flow_group_id}')
    client = Client()
    client.graphql(
        """
        mutation {
        delete_flow_group(input: {flow_group_id: "%s"}) {
            success
        }
        }
        """ % flow_group_id
    )

driver.close()
```