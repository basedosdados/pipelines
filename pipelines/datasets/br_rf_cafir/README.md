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
- **Pipeline:** `pipelines/datasets/br_rf_cafir/` (`constants.py`, `utils.py`,
  `tasks.py`, `flows.py`) — segue a estrutura padrão. O antigo
  `pipelines/crawler/rf_cafir/` foi removido; todo o código foi migrado.
- **Flow:** `pipelines/datasets/br_rf_cafir/flows.py` — cron diário (`0 0 * * *`)
- **Permissionamento:** `PartBdpro` sobre `data_referencia` (janela recente paga)

---

## Arquitetura do flow

1. `get_api_metadata` faz **uma única requisição PROPFIND** à pasta WebDAV e
   monta um DataFrame unificado com, para cada arquivo: `nome_arquivo`,
   `data_referencia` (extraída do nome do arquivo, ex. `K34313UF.D60701...` →
   `2026-07-01`) e `data_modificacao` (o `getlastmodified` daquele arquivo
   específico, devolvido pela própria resposta WebDAV).
2. `get_last_reference_date`/`decide_files_to_download` decidem qual
   `data_referencia` processar — a mais recente por padrão, ou uma data
   explícita via parâmetro `data_referencia` do flow.
3. `poll_source_for_update_task` compara essa `data_referencia` contra a
   cobertura já registrada (`compare_against="coverage"`) para decidir se há
   novidade.
4. `commit_source_update_task` já é chamado **antes** do download/materialização
5. `download_file.map(...)` e `process_file.map(...)` baixam e processam os
   arquivos da UF em paralelo, um por task, limitados a 6 workers simultâneos
   (`ThreadPoolTaskRunner(max_workers=6)`). Downloads reusam uma única `requests.Session` com pool de conexões (`utils.py::_session`) e gravam em disco via streaming (`stream=True`), em vez de carregar o arquivo inteiro em memória.
6. Cada arquivo processado grava sua própria `data_modificacao` (a do arquivo,
   não mais um máximo do diretório) como coluna do dado, e é particionado no
   Storage por `data_referencia` (`data=YYYY-MM-DD/`).

---

## Particionamento no Storage — corrigido

Antes desta correção, tanto a partição do Storage quanto a coluna de
comparação usavam a **data de modificação do arquivo no servidor**, tomada
como o `max()` de `getlastmodified` sobre *todos* os arquivos da pasta. Isso
divergia da data usada pelo poll/commit de metadados (a data do **nome do
arquivo**), e criava um risco real: se a Receita mexesse em qualquer arquivo
da pasta — inclusive de anos antigos — a data de modificação avançava, a
partição era gravada sob uma data nova, e o incremental reingeria o dataset
inteiro como período novo, arrastando junto o corte livre/pago do `PartBdpro`.
(Isso estava registrado na issue #1696.)

**A correção:** o particionamento e a comparação passaram a usar
`data_referencia`, extraída do **nome do arquivo**, em vez da data de
modificação do servidor. A antiga `data_modificacao` (antes uma coluna
derivada do máximo do diretório) agora é capturada **por arquivo**, a partir
do metadado retornado pela própria API para aquele arquivo específico, e
persistida como coluna adicional na tabela (`data_modificacao`) — apenas informativa, sem mais afetar partição nem o incremental.

**Dados históricos (antes de 12/2025):** como a coluna `data_modificacao` não
existia nos dados já materializados, o histórico anterior a dezembro/2025 foi
migrado a partir da pasta de staging no projeto `basedosdados`, preenchendo
`data_modificacao` com o mesmo valor de `data_referencia` daquele período (não
havia como recuperar a data de modificação real do servidor retroativamente).
A partir de 12/2025 em diante, `data_modificacao` reflete o `getlastmodified`
real de cada arquivo, capturado pelo flow.

---

## Cuidado ao Operar

Os defaults do flow escrevem em **prod**: `target="prod"`,
`materialize_after_dump=True`, `update_metadata=True`. Um disparo manual com
`{}` materializa em prod e reaplica a Row Access Policy do `PartBdpro`.

Parâmetros seguros para teste em dev:
`{"materialize_after_dump": False, "update_metadata": False, "force_run": True}`.

Para reprocessar uma data de referência específica (em vez da mais recente
disponível na fonte), passe `data_referencia="YYYY-MM-DD"`.
