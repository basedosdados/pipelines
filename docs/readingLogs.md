# Como obter logs completos para um Flow do Prefect

No monitoramento de flows em desenvolvimento ou em produção é muito importante ter acesso aos logs para que os processos de debug e *troubleshooting* possam ser feitos de forma eficiente. Você pode acessar os logs do seu Flow na UI do prefect na aba "logs". Alternativamente, os logs do mesmo Flow podem ser vistos com um pouco mais de detalhe no [Cloud Logging](https://console.cloud.google.com/logs/query) com a seguinte query:

```
resource.type="k8s_container"
resource.labels.project_id="basedosdados-dev"
labels."k8s-pod/prefect_io/flow_id"="[flow_id]"
```

Onde flow_id é o ID do seu Flow que pode ser encontrado na UI do prefect.

Ocorre que muitas vezes nos deparamos com logs pouco informativos. Por exemplo, quando há uma falha na materialização de uma tabela via DBT, frequentemente observamos o seguinte log:

```
Exception: There are no results here. This probably indicates that the task failed.
```

Nesse caso, é possível usar a CLI `kubectl` para obter logs mais completos. Para usar essa CLI você precisa fazer a instalação seguindo o passo-a-passo descrito na [documentação](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl) da GCP. Após a instalação, basta rodar o seguinte comando para obter os logs da materialização em dev:

```bash
kubectl logs -n prefect deploy/dbt-rpc-dev
```

Para ver os logs em prod, o comando é similar:

```bash
kubectl logs -n prefect-agent-basedosdados deploy/dbt-rpc-prod
```

Uma dificuldade que pode surgir no uso do `kubectl` é o volume de logs obtido com o comando. Uma dica para lidar de forma eficiente com o volume de informações é obtidas é usar a ferramenta `jq`, um processador de arquivos JSON. Com o `jq` você pode imprimir os logs formatados e aplicar filtros para encontrar a informação que você precisa para debugar o código.

Por exemplo, se eu estiver tendo problemas para materializar a tabela `br_ibge_ipca.mes_brasil`, posso usar o `jq` para ver apenas os logs cujo o campo `message` contém o nome da tabela de interesse:

```bash
kubectl logs -n prefect deploy/dbt-rpc-dev | jq -s | jq 'map(select(has("message")))' | jq 'map(select(.message | test("br_ibge_ipca.mes_brasil")?))'
```

O que, no presente caso, retorna:

```bash
  {
    "timestamp": "2022-05-27T23:16:45.654495Z",
    "message": "The selection criterion 'br_ibge_ipca.mes_brasil' does not match any nodes",
    "channel": "dbt",
    "level": 13,
    "levelname": "WARNING",
    "thread_name": "MainThread",
    "process": 188,
    "extra": {
      "context": "server",
      "run_state": "internal"
    }
  }
```

