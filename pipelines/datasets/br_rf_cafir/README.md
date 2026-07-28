# Documentação do Conjunto de Dados: CAFIR (Cadastro de Imóveis Rurais)

Este documento registra o contexto e as decisões sobre a base do CAFIR, para
futuros mantenedores.

---

## Sobre o Sistema

O CAFIR é o cadastro de imóveis rurais da Receita Federal. A base é publicada
numa pasta que a Receita expõe pela web (WebDAV), com um arquivo CSV por UF,
e é atualizada com frequência **diária**.

- **Tabela dbt:** `br_rf_cafir__imoveis_rurais` → `basedosdados.br_rf_cafir.imoveis_rurais`
- **Fonte:** `arquivos.receitafederal.gov.br/public.php/dav/files/.../CAFIR/`
- **Crawler:** `pipelines/crawler/rf_cafir/`
- **Flow:** `pipelines/datasets/br_rf_cafir/flows.py` — cron diário (`0 0 * * *`)
- **Permissionamento:** `PartBdpro` sobre `data_referencia` (janela recente paga)

---

## Particionamento no Storage — atenção

A partição do Storage (`data=YYYY-MM-DD/`) e a coluna `data_referencia` que
dela deriva **não** guardam a data de referência do dado. Guardam a **data de
modificação do arquivo no servidor** — o `max()` de `getlastmodified` sobre
*todos* os arquivos da pasta (`crawler/rf_cafir/utils.py::get_last_update_date`).

O poll e o commit de metadados, por outro lado, usam a data do **nome do
arquivo** (`K34313UF.D60701...` → `2026-07-01`). As duas datas divergem.

**Risco:** `data_referencia` é a chave do modelo incremental. Se a Receita
mexer em qualquer arquivo da pasta — inclusive de anos antigos — a data de
modificação avança, a partição é gravada sob data nova e o incremental reingere
o dataset inteiro como período novo. O corte livre/pago do `PartBdpro` se move
junto.

**Registrado na issue #1696.** A correção (particionar pela data do nome) foi
deixada para decisão do time, por alterar o significado de dados já
materializados — pendente definição sobre o histórico (manter × full-refresh).

---

## Problemas Identificados e Correções (jul/2026)

### 1. Flow falhava após ~57 min de download (`dbt_alias`)

**Sintoma:** a tabela parou de atualizar (última carga 2026-06-03) apesar do
cron diário. O run `cyan-macaw` (2026-07-15) baixava os ~40 arquivos por UF
(56 min), subia o staging com sucesso, e então `run_dbt` morria em 0 s com
`FileNotFoundError: Modelo dbt não encontrado: models/br_rf_cafir/imoveis_rurais.sql`.

**Causa:** `run_dbt` monta o caminho do modelo como
`f"{dataset_id}__{table_id}.sql" if dbt_alias else f"{table_id}.sql"`. O flow
declarava `dbt_alias=False`, procurando `imoveis_rurais.sql`, mas o arquivo real
segue a convenção da casa: `br_rf_cafir__imoveis_rurais.sql`. Introduzido na
migração Prefect 0→3.

**Agravante:** o commit do `Update` da fonte é a última etapa do flow, depois do
`run_dbt` que quebrava. Como a base é diária, o poll respondia "há novidade"
toda noite → baixava ~40 arquivos → morria no mesmo `if` → nunca registrava
nada. ~1 h de banda/CPU por dia batendo na Receita para produzir zero linhas.

**Correção:** `dbt_alias=True` no flow.

### 2. Requisição HTTP no import do módulo

**Causa:** `get_last_update_date` rodava no **nível de módulo** de
`crawler/rf_cafir/tasks.py` — ou seja, importar o módulo disparava uma request
à Receita. `load_flows_from_file` engole erros de import e retorna `{}`, então
se a fonte caísse no momento do deploy, o flow seria **descartado em silêncio**,
sem nada falhar visivelmente. (Mesma fonte que já nos derrubou por WAF no
`br_rf_cno`.)

**Correção:** movida para a task `task_get_last_update_date`, com a data passada
como parâmetro para `task_decide_files_to_download` e `task_download_files`. O
comportamento (qual data nomeia a partição) é preservado — este fix não toca em
dado; a mudança de *qual* data usar é a issue #1696, separada.

---

## Cuidado ao Operar

Os defaults do flow escrevem em **prod**: `target="prod"`,
`materialize_after_dump=True`, `update_metadata=True`. Um disparo manual com
`{}` materializa em prod e reaplica a Row Access Policy do `PartBdpro`.

Parâmetros seguros para teste em dev:
`{"materialize_after_dump": False, "update_metadata": False, "force_run": True}`.
