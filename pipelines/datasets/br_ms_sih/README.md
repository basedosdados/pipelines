# Documentação do Conjunto de Dados: SIH (Sistema de Informações Hospitalares do SUS)

Este documento registra o contexto e as decisões do conjunto `br_ms_sih`, para quem for
mantê-lo depois.

---

## Sobre o Sistema

O SIH registra as internações hospitalares pagas pelo SUS. O DATASUS publica os dados
mensalmente no FTP público, em arquivos `.dbc` por grupo e por UF, sob
`dissemin/publicos/SIHSUS/200801_/Dados/`.

| Tabela | Grupo no FTP |
|---|---|
| `aihs_reduzidas` | `RD` |
| `servicos_profissionais` | `SP` |

O nome do arquivo traz o grupo, a UF e a competência no formato `AAMM`: `RDSP2507.dbc` é
o grupo RD de SP em 2025-07.

## Estrutura no repositório

| Arquivo | Papel |
|---|---|
| `pipelines/datasets/br_ms_sih/flows.py` | Flows e agendamentos das duas tabelas |
| `pipelines/datasets/br_ms_sih/tasks.py` | Listagem do FTP, download e conversão para parquet |
| `pipelines/datasets/br_ms_sih/constants.py` | Ids das tabelas e regra de acesso BD Pro (`PartBdpro`) |
| `pipelines/shared/datasus/tasks.py` | Tasks de FTP, descompressão e conversão, compartilhadas com o SIA e o SINAN |
| `models/br_ms_sih/` | Modelos dbt das duas tabelas e do `dicionario` |

## Nome dos arquivos parquet

A conversão para parquet é feita por `read_dbf_save_parquet_chunks`, função
compartilhada pelo SIH, pelo SIA e pelo SINAN. O `tasks.py` do conjunto passa
`name_by_source_file=True`, então cada parquet leva o nome do arquivo do FTP de onde
saiu:

```text
ano=2025/mes=7/sigla_uf=SP/aihs_reduzidas_RDSP2507_0.parquet
```

O número final é o pedaço do arquivo: a conversão grava de 100 mil em 100 mil linhas.

A staging é carregada com `dump_mode="append"`, que só acrescenta arquivos e nunca apaga.
Com o nome tirado do arquivo do FTP, recarregar um mês sobrescreve os parquets que já
estão lá. Se o nome dependesse da posição do arquivo na lista do FTP, como no padrão
antigo, uma lista diferente para o mesmo mês geraria nomes novos, e o mês ficaria em
dobro na staging.

O nome pelo arquivo não cobre dois casos, em que os arquivos antigos continuam na staging
ao lado dos novos:

- **UF republicada com outro nome de arquivo**: os parquets do nome anterior
  continuam na staging.
- **Mês no padrão antigo**: os meses carregados antes desta convenção estão como
  `<tabela>_<posição>_<pedaço>.parquet`.

Nos dois casos, a pasta do mês precisa ser apagada na staging antes da recarga.

## Recarregar um mês

Os modelos `br_ms_sih__aihs_reduzidas` e `br_ms_sih__servicos_profissionais` são
incrementais: cada `dbt run` insere só os meses posteriores ao maior mês que já está na
tabela. Recarregar a staging de um mês passado não muda a tabela. Para refazer esse mês na
tabela, é preciso apagá-lo e inseri-lo de novo a partir da staging.
