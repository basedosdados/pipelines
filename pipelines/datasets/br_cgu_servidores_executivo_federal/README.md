# Documentação do Conjunto de Dados: br_cgu_servidores_executivo_federal (Servidores do Executivo Federal)

Este documento centraliza informações de contexto sobre a base de dados de servidores públicos do Executivo Federal, consolidando as particularidades da fonte e as decisões de engenharia adotadas para manter a atualização das tabelas. Serve de referência para futuros mantenedores.

---

## Sobre o Sistema

Os dados são publicados mensalmente pela Controladoria-Geral da União (CGU) no Portal da Transparência do Governo Federal. Cada mês é disponibilizado como um arquivo ZIP por subsistema (pacote), gerado sob demanda pelo portal.

- [Portal da Transparência — Servidores](https://portaldatransparencia.gov.br/download-de-dados/servidores)

Os dados de origem são segmentados em **subsistemas** (SIAPE, BACEN, Militares, DEFESA, Reserva/Reforma). Cada tabela do conjunto combina um ou mais desses subsistemas (ver seção *Tabelas e Subsistemas*).

---

## Particularidades da Fonte (pontos críticos)

Estas características do Portal da Transparência causaram a defasagem que travou a atualização das tabelas (ver o issue #2279 em [`pipelines/shared/cgu/README.md`](../../shared/cgu/README.md)). Estão tratadas em `source_url_is_available` e `next_available_date` (`pipelines/shared/cgu/utils.py`).

### 1. Bloqueio por falta de User-Agent (HTTP 405)

O portal responde **HTTP 405** a requisições sem `User-Agent` de navegador e, de forma intermitente, também a requisições com ele. O 405 **não diz que o arquivo não existe**.

**Tratamento:** todas as requisições do check_update enviam `User-Agent` de Chrome (`BROWSERS_USER_AGENT["chrome"]`) e o 405 é tratado como `TEMPORARY`, nunca como mês inexistente.

### 2. Geração assíncrona do ZIP (HTTP 202)

O portal prepara o arquivo de forma assíncrona: responde **HTTP 202** enquanto gera o ZIP e **200** (via redirect) quando está pronto.

**Tratamento:** `source_url_is_available` repete a consulta enquanto a resposta for 202. Esgotadas as tentativas, o resultado é `TEMPORARY`.

### Como cada resposta é classificada

`source_url_is_available` devolve um `SourceStatus`:

| Resposta | Status | Repete? |
|----------|--------|---------|
| 200 | `AVAILABLE` | não |
| 405 | `TEMPORARY` (após esgotar as tentativas) | sim |
| 202 esgotado | `TEMPORARY` | sim |
| 429 | `TEMPORARY` (após esgotar as tentativas) | sim |
| 5xx | `TEMPORARY` (após esgotar as tentativas) | sim |
| erro de rede / timeout | `TEMPORARY` (após esgotar as tentativas) | sim |
| 404 e qualquer outro código | `NOT_PUBLISHED` | não |

Só `NOT_PUBLISHED` faz o flow pular para o mês seguinte. Os detalhes (e os motivos) estão no [README de `shared/cgu`](../../shared/cgu/README.md).

> **Log de erro que originava a defasagem:** a URL do mês corrente retornava 405, o gate registrava `A URL {url} não existe!` e o flow encerrava sem persistir dados, mês após mês.

---

## Disponibilidade "tudo-ou-nada"

O Portal publica os subsistemas de um mesmo mês em **datas escalonadas**. Uma tabela que combina vários subsistemas (ex.: `observacoes`, com 9 pacotes) só é considerada disponível quando **todos** os seus subsistemas estão `AVAILABLE`. A regra está em `_month_status` (`pipelines/shared/cgu/utils.py`): basta um subsistema `NOT_PUBLISHED` para o mês inteiro ser `NOT_PUBLISHED`.

- **Decisão de produto:** **não há ingestão parcial de mês**. Um mês só entra quando está completo, evitando materializações incompletas.
- Quando o mês está `NOT_PUBLISHED`, o `check_update` não encontra mês novo e o `extract_and_load` não roda. A próxima execução tenta de novo.

---

## Estrutura dos Flows

Cada tabela tem um par de flows (`check_update` e `extract_and_load`) em `pipelines/datasets/br_cgu_servidores_executivo_federal/flows.py`, montado por `make_pipeline(table_id)` e agendado em horários distintos para escalonar a carga.

- O `check_update` (`get_latest_update`, em `tasks.py`) descobre o `reference_date` com `next_available_date` e compara com a cobertura da tabela.
- O `extract_and_load` recebe só o `reference_date` em `download_params`, baixa os ZIPs do mês (`download_file`) e particiona (`partition_data`).

Para testar em dev sem tocar prod, rode os deployments de dev e confira o `target` antes de executar: com `target="prod"` o flow grava no bucket de **produção**, mesmo rodando no worker de dev.

---

## Tabelas e Subsistemas

Mapeamento definido em `get_source` (`pipelines/shared/cgu/utils.py`).

| Tabela | Subsistemas combinados |
|--------|------------------------|
| `cadastro_servidores` | Servidores SIAPE, Servidores BACEN, Militares |
| `cadastro_aposentados` | Aposentados SIAPE, Aposentados BACEN |
| `cadastro_pensionistas` | Pensionistas SIAPE, Pensionistas BACEN, Pensionistas DEFESA |
| `cadastro_reserva_reforma_militares` | Reserva/Reforma Militares |
| `remuneracao` | Militares, Reserva/Reforma Militares, Servidores SIAPE, Servidores BACEN, Pensionistas BACEN, Pensionistas DEFESA (6 pacotes) |
| `afastamentos` | Servidores SIAPE, Servidores BACEN |
| `observacoes` | Todos os 9 subsistemas (Aposentados SIAPE/BACEN, Servidores SIAPE/BACEN, Pensionistas SIAPE/BACEN/DEFESA, Militares, Reserva/Reforma) |

As tabelas com mais subsistemas (`observacoes`, `remuneracao`) são as mais sensíveis ao atraso de publicação, pois dependem de que **todos** os pacotes do mês estejam prontos.

---

## Histórico de Correções

- **Destravamento da atualização (PR #1623):** substituição de `requests.get(url)` cru por `source_url_is_available` no gate e no download, tratando 405 (User-Agent) e 202 (ZIP assíncrono). Validado end-to-end em dev nas 7 tabelas. Mesma abordagem aplicada anteriormente em `br_cgu_beneficios_cidadao`.

