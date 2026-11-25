# Documentação do Conjunto de Dados: SICAR (Sistema Nacional de Cadastro Ambiental Rural)

Contexto e particularidades da base do CAR para quem for dar manutenção no flow.

---

## Sobre o Sistema

O SICAR é o sistema do Serviço Florestal Brasileiro que reúne os registros do Cadastro
Ambiental Rural. Os polígonos dos imóveis rurais são publicados por unidade federativa,
em shapefile compactado.

- [Portal de download](https://consultapublica.car.gov.br/publico/imoveis/index)

O download usa o pacote [`SICAR`](https://pypi.org/project/SICAR/), que resolve o captcha
do portal. Cada UF é baixada separadamente e o flow percorre as 27 em sequência
(`Constants.UF_SIGLAS`).

## Estrutura da tabela

Só existe uma tabela de dados, `area_imovel`, mais o `dicionario`.

O particionamento é `data_extracao` (data em que baixamos) + `sigla_uf`. A fonte não
versiona os arquivos: cada UF traz sempre o estado atual do cadastro, e a data de
referência de cada UF é lida do próprio portal por `get_each_uf_release_date`. Ou seja,
uma partição `data_extracao` é um retrato do CAR no dia da execução, não um recorte
temporal da fonte.

A coluna `geometry` vem em WKT no parquet e é convertida para `GEOGRAPHY` no modelo dbt.

## BD Pro

`area_imovel` é `PartBdpro` sobre `data_extracao`, com o `free_lag` padrão de 6 meses. As
extrações dos últimos 6 meses ficam atrás do plano pago; as anteriores são abertas.

---

## Armadilhas

### 1. `dbt_alias` precisa ser `True` — o arquivo do model tem prefixo

`run_dbt` monta o caminho do `.sql` a partir de `dataset_id`/`table_id` e falha se o
arquivo não existir:

```python
selected = models_folder / (
    f"{dataset_id}__{table_id}.sql" if dbt_alias else f"{table_id}.sql"
)
```

O model deste conjunto é `models/br_sfb_sicar/br_sfb_sicar__area_imovel.sql`, com
`alias="area_imovel"` no config — ou seja, **exige `dbt_alias=True`**.

No Prefect 0 o valor certo vinha do `parameter_defaults` do agendamento, não do flow: o
`Parameter` no arquivo era `False` e o schedule sobrescrevia com `True`. Na migração para
o Prefect 3 o `Parameter` virou default de função e o schedule virou um cron inline sem
parâmetros, então o `False` passou a valer. O resultado é um flow que baixa as 27 UFs,
gera o parquet, sobe para a staging e só então quebra com

```text
FileNotFoundError: Modelo dbt não encontrado: models/br_sfb_sicar/area_imovel.sql
```

Duas consequências práticas: a verificação do caminho acontece na hora da task de dbt,
depois de mais de uma hora de download, e a staging fica com dado novo que nunca virou
tabela.

A verificação existe porque o `dbtRunner` **retorna sucesso quando o arquivo do model não
existe** — sem ela, o flow terminaria verde sem materializar nada.

### 2. Não há poll da fonte

O flow não consulta a fonte antes de baixar: todo run agendado percorre as 27 UFs e
materializa. O parâmetro `force_run` está na assinatura mas não é usado por nada — não há
guarda para ele desligar.

Um run extra não corrompe a tabela, mas o que ele faz depende do dia. A partição é
`datetime.today()`, então um run no mesmo dia reaproveita a partição existente: o
`Storage.upload(..., if_exists="replace")` sobrescreve os blobs de mesmo nome, sem criar
partição nova nem duplicar linha. E o modelo é incremental com
`data_extracao > (select max(data_extracao) from {{ this }})` — maior, não maior ou igual
—, de modo que nada do run repetido chega à tabela. O custo é o download inteiro, em
troca de nada.

Dois efeitos colaterais do `replace`: ele não apaga blobs sem correspondência, então o
parquet de uma UF que suma da fonte permanece no prefixo e continua sendo lido; e um run
em outro dia cria, aí sim, uma partição a mais.

### 3. O download é lento e falha sozinho

`download_car` tem `retries=3` no Prefect e `max_retries=8` internamente, para timeout de
leitura. UFs grandes (MG, BA, RS) frequentemente consomem essas tentativas. O run completo
das 27 UFs leva por volta de uma hora e meia.

### 4. O projeto da staging vem do pod, não do `bucket_name`

O primeiro `upload_to_gcs` do flow recebe `bucket_name="basedosdados-dev"`, mas o projeto
BigQuery da staging não sai daí: a lib `basedosdados` o resolve pelo `config.toml` do pod,
em `gcloud-projects.staging.name` — `basedosdados-dev` no worker de dev,
`basedosdados-staging` no de prod. Rodar o flow no pool de prod faz o upload conferir e
alterar a tabela de `basedosdados-staging` enquanto escreve no bucket de dev. Run de
verificação tem que sair do pool `basedosdados-dev`.

### 5. A fonte traz colunas que a tabela externa não declara

O SICAR passou a publicar `dat_criaca` e `dat_atuali` entre dez/2024 e mai/2025. A tabela
externa `basedosdados-staging.br_sfb_sicar_staging.area_imovel` foi criada em out/2024 e
declara as onze colunas anteriores, mais `data_atualizacao_car` e as duas de partição.
Como uma tabela externa lê apenas o que está declarado, as duas colunas novas são
ignoradas, e o modelo não as usa — a divergência não afetou nenhum run até 07/2026, quando
o `_sync_staging_schema` passou a comparar os dois schemas e a tentar acrescentar ao
BigQuery o que falta.

Como o PATCH falha (ver Pendências), qualquer coluna nova na fonte derruba o upload, mesmo
sem nenhum modelo lendo essa coluna — e derruba depois do download inteiro.

---

## Pendências

- **O PATCH do schema da staging falha com 403.** `_sync_staging_schema`
  (`pipelines/utils/tasks.py`) abre `bigquery.Client(project=...)` sem credencial e cai no
  ADC do pod, que só lê; o `get_table` passa e o `update_table` estoura com
  `bigquery.tables.update denied`. O cliente autenticado está em
  `tb.client["bigquery_staging"]`, no objeto que a função já recebe. Vale para este e para
  qualquer outro conjunto; o conserto sai em PR à parte.
- **Não há staging em dev.** Nem o dataset `br_sfb_sicar_staging` em `basedosdados-dev`,
  nem o prefixo `gs://basedosdados-dev/staging/br_sfb_sicar/`. O próximo run no pool de dev
  os cria pelo ramo `tb.create` do `upload_to_gcs`.
- **A tabela de prod está parada em `data_extracao=2025-10-15`**, o último run
  bem-sucedido, anterior à migração para o Prefect 3.
