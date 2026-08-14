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

O geometry vem em WKT no parquet e é convertido para `GEOGRAPHY` no modelo dbt.

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

Como a fonte não versiona os arquivos e cada extração vira uma partição nova, um run
extra não corrompe a tabela; só custa o download inteiro e cria uma partição a mais.

### 3. O download é lento e falha sozinho

`download_car` tem `retries=3` no Prefect e `max_retries=8` internamente, para timeout de
leitura. UFs grandes (MG, BA, RS) frequentemente consomem essas tentativas. O run completo
das 27 UFs leva por volta de uma hora e meia.
