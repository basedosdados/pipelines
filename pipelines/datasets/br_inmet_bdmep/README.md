# Documentação BDMEP/INMET (Banco de Dados Meteorológicos)

Este documento registra informações importantes sobre a base de dados do BDMEP, Instituto Nacional de Meteorologia (INMET), esclarecendo o estado de estruturação da pipeline, problemas enfrentados e instruções para entendimento do fluxo de trabalho.

---

## Sobre a fonte

O BDMEP reúne dados meteorológicos diários de séries históricas das várias estações meteorológicas convencionais da rede de estações do INMET com milhões de informações, referentes às medições diárias. 

- [Fonte de Dados](https://portal.inmet.gov.br/dadoshistoricos)
- [Página Inicial do Conjunto](https://bdmep.inmet.gov.br)

**Frequência de atualização:** 90 dias

## Extração
Os dados são divulgados trimestralmente e encontram-se agrupados por ano em arquivos `.zip`. Cada arquivo zip anual possui um `.csv` para cada Estação Meteorológica, nomeado no padrão `f"INMET_{sigla_regiao}_{sigla_uf}_{id_estacao}_{estacao}_{data_inicio}_A_{data_fim}.CSV`.

### Estações 

As 8 primeiras linhas de cada arquivo, contém informações de cabeçalho sobre a estação:
- id_estacao
- estacao (nomn da estação)
- sigla_uf
- data_fundacao
- latitude
- longitude
- altitude

Essas informações são extraídas do _flow_ `br_inmet_estacao`. Além disso, acontece um cálculo de distâncias entre a latitude e longitude da estação com os centróides das cidades (dados presentes nos diretórios brasileiros, `br_bd_diretorios_brasil__municipio`) no mesmo estado. Aquele município cujo centroide fica menos distante da estação fornece o `id_municipio` que será atribuído à estação.

#### Storage
**Particionamento**: Nenhum

#### Materialização
A materialização segue a lógica de recriação da tabela a cada execução. 
A arquitetura da tabela encontra-se 

### Microdados

#### Lógica incremental
Na coleta, a lógica incremental se dá de duas formas:
1. Verificação da atualidade dos dados na fonte em comparação com o BigQuery (a _task_ clássica `check_if_data_is_outdated`).
Aqui, no entanto, existe uma particularidade: foi adicionada a possibilidade de suspender essa checagem com o parâmetro `check_for_updates` e executar a coleta completa (não incremental) para um ano inteiro, determinado pelo parâmetro `year`.
Isso porque no processo de correção do flow, foi necessário reinserir os dados de 2025 que antes estavam duplicados e tinham sido corrigidos. Mas com a checagem tradicional de `check_if_data_is_outdated`, a extração não acontecia (porque em tese, a última data atualização no BigQuery era igual ou superior à data de última atualização na fonte).

```python
check_for_updates = Parameter(
        "check_for_updates", default=True, required=False
    )
```

2. Remoção de duplicatas e _warning_ no momento de salvar os dados: a função `verify_inmet_duplicates` avalia a combinação única de `["id_estacao", "data", "hora"]` para identificar duplicatas.


#### Storage
**Particionamento**: ano

#### Materialização
A materialização também segue uma lógica incremental através de alguns recursoss:
1. O parâmetro de configuração com `materialized="incremental"` e `unique_key=["data", "hora", "id_estacao"]`;
2. O uso de:
```sql
{% if is_incremental() %}
    where
        safe_cast(data as date) >= (select max(data) from {{ this }})
        and safe_cast(ano as int64) >= (select max(ano) from {{ this }})
{% endif %}
```

para evitar a leitura e comparação com toda a tabela;

3. O uso de `qualify row_number() over (partition by {{ unique_keys | join(", ") }}) = 1` para gerar apenas uma linha para cada estação, data e hora.
---

## Tabelas e Particularidades

### br_inmet_bdmep__microdados

**Problemas Identificados:**
1. Duplicatas em todo o ano de 2025 (duas linhas para cada estação, data e hora).
2. Impossibilidade de executar a pipeline excepcionalmente para um certo ano e reinserir os dados;
3. Erro no BigQuery ao materializar, tendo usado `pipelienes.utils.utils.to_partitions`; 


**[Log de Erro:](https://prefect.basedosdados.org/default/flow-run/f1db1ac3-092a-40f7-b5e3-4191e6721684?logs)**
```bash
DBT ERROR [15:20:49.941844]: [MainThread]:   Enum type name not found in the specified DescriptorPool: googlesql.functions.DateTimestampPart
```
**Decisões e Tratamento:**

1. Lógica incremental (no código Python e na materialização, como já mencionado acima) adição de testes no `schema.yml` para combinações únicas das colunas de data, hora e estação;

2. Uso do parâmetro booleano `check_for_updates` (verdadeiro, por padrão);

3. Implementação de uma função `to_partitions` própria, que salve os dados com o nome já padronizado para os anos anteriores (`f"microdados_{ano}.csv"`) e não como estava (`"data.csv"`);

### br_inmet_bdmep__estacao
Os pontos de atenção são o fato de os dados de estação estarem no cabeçalho dos arquivos de microdados e que a identificação com um município acontece pelo cálculo da distância da estação para o centroide de todos os municípios do estado correspondente.

---

## Últimas alterações
* [Issue](https://github.com/basedosdados/pipelines/issues/1461)
* [PR](https://github.com/basedosdados/pipelines/pull/1515)
