# Documentação relacionado ao Projeto de parceria entre a Base Dos Dados e Sou da Paz.

## Estrutura do Projeto

A estrutura completa do projeto — incluindo ingestão, tratamento e modelagem dos dados — está disponível no repositório da Base dos Dados (https://github.com/basedosdados/pipelines
). No diretório models/br_sou_da_paz_armas_municoes/, a equipe da Sou da Paz encontrará todos os componentes necessários para compreensão e manutenção do pipeline: os scripts de tratamento em Python estão localizados em code/*.py, concentrando a lógica de ingestão e transformação inicial; os modelos em SQL, utilizados via dbt, estão nos arquivos *.sql, responsáveis pela modelagem analítica das tabelas; e o arquivo schema.yml documenta os schemas, contendo descrições de tabelas e colunas, além dos testes de qualidade e integridade aplicados aos dados.

O fluxo operacional do projeto se inicia com a ingestão dos dados a partir das planilhas mantidas pela equipe da Sou da Paz. A função download_file, implementada em models/br_sou_da_paz_armas_municoes/code/main.py, realiza o download direto desses arquivos a partir da pasta [Planilhas Finais](https://drive.google.com/drive/u/1/folders/1lSk7Pe_pHlnTi5ywCTEJOAeOg2ws2UYN) no Google Drive, que deve estar previamente organizada conforme o padrão acordado para garantir a correta leitura dos dados. Após a ingestão, os dados passam por uma etapa de tratamento em Python, na qual são realizadas transformações com foco em padronização de formatos, limpeza de inconsistências, normalização de categorias e preparação geral para uso analítico.

Concluída essa etapa, os dados tratados são enviados para a infraestrutura da Base dos Dados. Inicialmente, são armazenados em um bucket privado no Google Cloud Storage chamado [basedosdados-consultoria](https://console.cloud.google.com/storage/browser/basedosdados-consultoria/sou_da_paz;tab=objects?prefix=&forceOnObjectsSortingFiltering=false), garantindo persistência e versionamento. Em paralelo, são criadas tabelas no ambiente de staging do BigQuery, que servirão como base para a modelagem analítica. A partir desse ponto, entra a etapa de modelagem com dbt, na qual os dados são estruturados de forma mais refinada: definimos a tipagem adequada de cada coluna, padronizamos os modelos e aplicamos testes de qualidade, incluindo validações de integridade referencial com diretórios consolidados da Base dos Dados, como os de ano e sigla de unidade federativa, além de verificações para garantir que não há colunas totalmente nulas.

Após a validação, os dados tornam-se disponíveis no ambiente de desenvolvimento, já prontos para consumo. Na etapa final, essas tabelas são disponibilizadas para a equipe da Sou da Paz por meio da criação de views no ambiente da organização, funcionando como réplicas das tabelas mantidas pela Base dos Dados. Essa abordagem garante que os dados consumidos estejam sempre atualizados e alinhados com a fonte oficial, sem necessidade de duplicação física. Com isso, as tabelas ficam prontas para integração com ferramentas de business intelligence, como o Metabase, permitindo que a equipe concentre seus esforços diretamente nas análises.


## Como subir os dados

Para que os dados sejam publicados na Base dos Dados e posteriormente consumidos pelo projeto da Sou da Paz, é necessário que um Analista ou Engenheiro com acesso à service account do grupo `subidores-de-dados` esteja disponível. Esse acesso é indispensável para autenticação e permissões de escrita tanto no Storage quanto no BigQuery.

Com as credenciais devidamente configuradas, siga o fluxo abaixo:

### 1. Configuração do ambiente

No arquivo `.basedosdados/config.toml`, atualize o parâmetro `bucket_name` de `basedosdados-dev` para `basedosdados-consultoria`.

Essa alteração garante que os dados sejam enviados para o ambiente correto de consultoria.

---

### 2. Execução do pipeline de ingestão

No repositório de pipelines da Base dos Dados, execute:

```bash
uv run python3 models/br_sou_da_paz_armas_municoes/code/tabelas.py
```
Esse comando processa todas as tabelas do projeto, realiza o upload dos dados para o Cloud Storage e, na sequência, os disponibiliza no BigQuery em ambiente de staging.

---

### 3. Materialização dos modelos no BigQuery
Em seguida rode:
```bash
uv run dbt run --select models/br_sou_da_paz_armas_municoes/
```
Esse passo materializa os modelos definidos no dbt, criando ou atualizando as tabelas e views no dataset final.

---

### 4. Validação dos dados
Por fim, execute:

```bash
uv run dbt test --select models/br_sou_da_paz_armas_municoes/
```

Esse comando roda os testes definidos no projeto, validando integridade, consistência e regras de negócio das tabelas.

> [!WARNING]

> Como os modelos finais são materializados como views, qualquer atualização nos dados de origem ou ajustes nos dados será refletida automaticamente no projeto da Sou da Paz, sem necessidade de republicação manual adicional.

## Estrutura da VM

A máquina virtual (VM) é o ambiente computacional responsável por hospedar o Metabase da Sou da Paz. Em termos práticos, trata-se de um servidor provisionado na infraestrutura do Google Cloud, operando de forma dedicada para a aplicação.

Abaixo estão os detalhes da configuração atual:

- **Nome da instância:** `metabase-sou-da-paz`
- **Zona de hospedagem:** `us-central1-b`
- **Sistema operacional:** `Linux (Debian 12)`
- **Tipo de máquina:** `e2-small`
  - 2 vCPUs
  - 2 GB de memória RAM
- **Armazenamento:** 30 GB (disco persistente)

---

### Política de backup

O disco da VM possui uma política de backup configurada para garantir a recuperação em caso de falhas ou incidentes.

- **Frequência:** semanal
- **Dia:** sexta-feira
- **Horário:** entre 22:00 e 23:00

Esses backups são realizados automaticamente e armazenados como snapshots do disco.
