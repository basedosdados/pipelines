# Documentação do Conjunto de Dados: SIA (Sistema de Informações Ambulatoriais do SUS)

Este documento registra o contexto e as decisões do conjunto `br_ms_sia`, para quem for
mantê-lo depois.

---

## Sobre o Sistema

O SIA registra a produção ambulatorial do SUS. O DATASUS publica os dados mensalmente no
FTP público, em arquivos `.dbc` por grupo e por UF, sob
`dissemin/publicos/SIASUS/200801_/Dados/`.

| Tabela | Grupo no FTP |
|---|---|
| `producao_ambulatorial` | `PA` |
| `psicossocial` | `PS` |

O nome do arquivo traz o grupo, a UF e a competência no formato `AAMM`: `PASP2507a.dbc` é
o grupo PA de SP em 2025-07. UFs com muitos registros têm o mês dividido em partes
(`PASP2507a`, `PASP2507b`, …).

## Estrutura no repositório

| Arquivo | Papel |
|---|---|
| `pipelines/datasets/br_ms_sia/flows.py` | Flows e agendamentos das duas tabelas |
| `pipelines/datasets/br_ms_sia/tasks.py` | Listagem do FTP, download e conversão para parquet |
| `pipelines/datasets/br_ms_sia/constants.py` | Ids das tabelas e regra de acesso BD Pro (`PartBdpro`) |
| `pipelines/shared/datasus/tasks.py` | Tasks de FTP, descompressão e conversão, compartilhadas com o SIH e o SINAN |
| `models/br_ms_sia/` | Modelos dbt das duas tabelas e do `dicionario` |

## Nome dos arquivos parquet

A conversão para parquet é feita por `read_dbf_save_parquet_chunks`, função
compartilhada pelo SIA, pelo SIH e pelo SINAN. O `tasks.py` do conjunto passa
`name_by_source_file=True`, então cada parquet leva o nome do arquivo do FTP de onde
saiu:

```text
ano=2025/mes=7/sigla_uf=SP/producao_ambulatorial_PASP2507a_0.parquet
```

O número final é o pedaço do arquivo: a conversão grava de 100 mil em 100 mil linhas.

A staging é carregada com `dump_mode="append"`, que só acrescenta arquivos e nunca apaga.
Com o nome tirado do arquivo do FTP, recarregar um mês sobrescreve os parquets que já
estão lá. Se o nome dependesse da posição do arquivo na lista do FTP, como no padrão
antigo, uma lista diferente para o mesmo mês geraria nomes novos, e o mês ficaria em
dobro na staging e na tabela.

O nome pelo arquivo não cobre dois casos, em que os arquivos antigos continuam na staging
ao lado dos novos:

- **UF republicada com outro nome**: o FTP troca `PARS2603.dbc` por `PARS2603a.dbc` e
  `PARS2603b.dbc`.
- **Mês no padrão antigo**: os meses carregados antes desta convenção estão como
  `producao_ambulatorial_<posição>_<pedaço>.parquet`.

Nos dois casos, a pasta do mês precisa ser apagada na staging antes da recarga.

## Recarregar um mês

O modelo `br_ms_sia__producao_ambulatorial` é incremental: cada `dbt run` insere apenas o
mês seguinte ao maior mês que já está na tabela. Recarregar a staging de um mês passado
não muda a tabela. Para refazer esse mês na tabela, é preciso apagá-lo e inseri-lo de novo
a partir da staging.

## Linhas idênticas

O SIA/PA não tem identificador de registro, e os arquivos do FTP trazem linhas idênticas em
todas as colunas. Por isso o `producao_ambulatorial` não tem chave única nem teste de
unicidade de linhas.

Medição em 2025-07, todas as UFs:

| Onde | Colunas comparadas | Linhas | Distintas | Repetidas |
|---|---|---|---|---|
| Staging | 65 (todas as do arquivo, mais `ano`, `mes` e `sigla_uf`) | 70.903.570 | 49.822.817 | 21.080.753 |
| Tabela | 59 (as do modelo) | 70.903.570 | 49.122.952 | 21.780.618 |

As 699.865 repetidas a mais na tabela vêm das colunas que o modelo descarta ou transforma.
Na staging, a repetição aparece em todas as UFs, de 15% no PI (47.517 de 314.945) a 39% no
MT (817.257 de 2.078.910).

```sql
select
    sigla_uf,
    count(*) as linhas,
    count(distinct to_json_string(t)) as linhas_distintas,
    count(*) - count(distinct to_json_string(t)) as linhas_repetidas
from `basedosdados-staging.br_ms_sia_staging.producao_ambulatorial` as t
where ano = '2025' and mes = '7'
group by rollup (sigla_uf)
order by sigla_uf nulls first
```

```sql
select
    count(*) as linhas,
    count(distinct to_json_string(t)) as linhas_distintas,
    count(*) - count(distinct to_json_string(t)) as linhas_repetidas
from `basedosdados.br_ms_sia.producao_ambulatorial` as t
where ano = 2025 and mes = 7
```
