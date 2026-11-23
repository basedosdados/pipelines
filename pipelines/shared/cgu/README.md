# Lógica compartilhada dos conjuntos CGU (`pipelines/shared/cgu`)

Este módulo serve os quatro conjuntos baixados do Portal da Transparência da CGU:

| Conjunto | README |
|----------|--------|
| `br_cgu_servidores_executivo_federal` | [README](../../datasets/br_cgu_servidores_executivo_federal/README.md) |
| `br_cgu_cartao_pagamento` | [README](../../datasets/br_cgu_cartao_pagamento/README.md) |
| `br_cgu_licitacao_contrato` | [README](../../datasets/br_cgu_licitacao_contrato/README.md) |
| `br_cgu_beneficios_cidadao` | [README](../../datasets/br_cgu_beneficios_cidadao/README.md) |

Todos têm o mesmo desenho: um par de flows por tabela (`check_update` → `extract_and_load`), com as tasks de `tasks.py` e as funções de `utils.py` deste módulo. As particularidades de cada conjunto ficam no README dele.

---

## Como os dois estágios se falam

- O `check_update` (`get_latest_update`, no `tasks.py` de cada conjunto) lê a última cobertura da tabela no backend (`last_date_in_metadata`), chama `next_available_date` e devolve `SourceInspection(reference_date=..., compare_against="coverage")`.
- O `extract_and_load` **usa só o `reference_date`** que o `check_update` devolveu (chega em `download_params["reference_date"]`).

Consequência: se o `check_update` escolher o mês errado, o `extract_and_load` baixa o mês errado. Por isso o `check_update` não pode pular um mês que a fonte talvez tenha.

---

## `SourceStatus`: o que cada resposta HTTP significa

`source_url_is_available` (`utils.py`) consulta uma URL e devolve um `SourceStatus` (`constants.py`):

| Resposta | `SourceStatus` | Observação |
|----------|----------------|------------|
| 200 | `AVAILABLE` | Arquivo pronto. |
| 202 | `TEMPORARY` (se esgotar) | O portal ainda está gerando o ZIP. Repete. |
| 405 | `TEMPORARY` (se esgotar) | Bloqueio do portal, intermitente. **Não diz se o arquivo existe.** Repete. |
| 429 | `TEMPORARY` (se esgotar) | Excesso de requisições. Repete. |
| 5xx | `TEMPORARY` (se esgotar) | Erro do portal. Repete. |
| Erro de rede ou timeout | `TEMPORARY` (se esgotar) | Repete. |
| 404 e qualquer outro código | `NOT_PUBLISHED` | O mês não existe na fonte. Não repete. |

Só `NOT_PUBLISHED` significa que o mês não existe. Tudo que não responde de forma definitiva é `TEMPORARY`. Os códigos 202, 405 e 429 estão em `_TEMPORARY_STATUS_CODES` (`constants.py`); 5xx e erro de rede entram pelo ramo `code >= 500` e pelo `except requests.RequestException`.

Ao adicionar um código novo, decida: ele prova que o arquivo não existe (404)? Se não prova, é `TEMPORARY`.

São até 5 tentativas por URL (`max_retries=5`). Cada tentativa espera 30 s se a resposta foi 202 (`wait_seconds`) ou 15 s nos demais casos temporários, e a requisição tem timeout de 30 s. No pior caso, uma URL sempre `TEMPORARY` leva cerca de 5 min. Um 200 espera 5 s antes de devolver. Um `NOT_PUBLISHED` volta na hora.

---

## `next_available_date`: qual mês carregar

`next_available_date(dataset_id, table_id, relative_month, last_date_in_api)`:

1. Parte da **última cobertura + `relative_month`** (hoje 1 mês em todos os conjuntos).
2. Calcula o status do mês (`_month_status`).
3. Se **não** for `NOT_PUBLISHED` (ou seja, `AVAILABLE` ou `TEMPORARY`), **devolve esse mês**.
4. Se for `NOT_PUBLISHED`, passa para o mês seguinte.
5. Para quando chega a `MAX_MONTH_RANGE` (6) passos ou à data de hoje. Se nenhum mês serve, devolve `last_date_in_api`, e o `check_update` entende que não há dado novo.

O que isso implica:

- **Um mês por execução, em ordem.** A função devolve o **primeiro** mês pendente, não o mais recente. Assim não nasce lacuna. Se a tabela está atrasada em vários meses, cada mês leva uma execução: o backfill leva várias execuções.
- **Meses `NOT_PUBLISHED` são pulados**, para uma lacuna real da fonte (um mês que o portal nunca publicou) não travar a tabela para sempre.
- **`TEMPORARY` não é pulado.** A fonte pode ter o mês e só não ter respondido. A função devolve o mês para o `extract_and_load` tentar baixar e, se falhar, falhar com o erro real.

---

## Regra "tudo ou nada" entre subsistemas (`_month_status`)

Algumas tabelas combinam vários arquivos por mês (no servidores, até 9 subsistemas; ver o README do conjunto). `_month_status` junta os status das URLs do mês assim:

- **Qualquer URL `NOT_PUBLISHED` → o mês é `NOT_PUBLISHED`.** A função para na primeira e não consulta as demais.
- Senão, **qualquer URL `TEMPORARY` → o mês é `TEMPORARY`.**
- Senão (todas `AVAILABLE`), o mês é `AVAILABLE`.

Não existe ingestão parcial de mês. O portal publica os subsistemas em datas escalonadas, então um mês só entra quando todos estão publicados.

---

## Retry do download (`download_file`)

`download_file` (`tasks.py`) baixa e extrai os ZIPs do `reference_date`. A task tem:

- `retries=DOWNLOAD_RETRIES` (5), ou seja, até **6 tentativas**;
- espera exponencial `15 * 2**n` s entre elas (`DOWNLOAD_RETRY_BACKOFF_SECONDS=15`): **15, 30, 60, 120 e 240 s**, somando 465 s (~8 min);
- jitter de 25% (`DOWNLOAD_RETRY_JITTER`), que desloca cada espera para mais ou para menos;
- `retry_condition_fn=_retry_if_transient`, que repete **só erro transitório** (`is_transient_download_error`): `HTTPError` 5xx, `URLError`, timeout, `ConnectionError` e `zipfile.BadZipFile` (ZIP truncado ou resposta 202 com corpo que não é ZIP). Qualquer outro erro falha na hora, sem gastar as esperas.

> **Atenção:** `HTTPError` com código abaixo de 500 (por exemplo 404, 405 e 429 no momento do download) **não** é repetido: `is_transient_download_error` só olha `code >= 500`. Isso difere do `check_update`, em que 405 e 429 são repetidos. A docstring de `is_transient_download_error` cita 405/429 como transitórios, o que não bate com o código. Se isso for mudado, atualize os dois.

### Quanto tempo o retry pode somar à task

- Esperas: ~465 s (~8 min), ±25% de jitter.
- Cada tentativa tem timeout de 120 s por URL (`urlopen(..., timeout=120)` em `download_and_unzip_file`).
- Para conjuntos com **uma URL por tabela**, o pior caso é de cerca de 6 × 120 s + ~8 min, ou seja, **~20 a 22 min**.
- No **servidores**, uma tabela baixa várias URLs na mesma task, e um retry **refaz todas**. Em `observacoes` (9 URLs), o pior caso passa de 1 hora e meia. Na prática só acontece com a fonte fora do ar.

---

## Issue #2279: 405 lido como mês inexistente

**O que aconteceu.** O portal passou a responder 405 de forma intermitente. O código antigo tratava qualquer resposta diferente de 200 e 202 como "URL não existe" (`NOT_PUBLISHED`, na linguagem de hoje). Cadeia de falhas:

1. O `check_update` recebeu 405 para o mês que existia e concluiu que ele não estava publicado.
2. Como o mês "não existia", a busca **avançou** até um mês realmente não publicado.
3. O `extract_and_load` tentou baixar esse mês e levantou `FileNotFoundError`, que **escondia o erro original** (o 405).
4. Resultado: as tabelas ficaram sem atualizar e o log apontava para o lugar errado.

**O que foi mudado**

- `source_url_is_available` devolve `SourceStatus` em vez de `bool`. Um `bool` não distingue "não existe" de "não respondeu".
- 405, 202 esgotado, 429, 5xx e erro de rede são `TEMPORARY`. Só 404 e outros códigos são `NOT_PUBLISHED`.
- `next_available_date` não avança sobre `TEMPORARY`, e carrega o primeiro mês pendente em vez do mais recente.
- O download tem retry com backoff, só para erro transitório.
