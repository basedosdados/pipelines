# Documentação do Conjunto de Dados: br_cgu_servidores_executivo_federal (Servidores do Executivo Federal)

Este documento centraliza informações de contexto sobre a base de dados de servidores públicos do Executivo Federal, consolidando as particularidades da fonte e as decisões de engenharia adotadas para manter a atualização das tabelas. Serve de referência para futuros mantenedores.

---

## Sobre o Sistema

Os dados são publicados mensalmente pela Controladoria-Geral da União (CGU) no Portal da Transparência do Governo Federal. Cada mês é disponibilizado como um arquivo ZIP por subsistema (pacote), gerado sob demanda pelo portal.

- [Portal da Transparência — Servidores](https://portaldatransparencia.gov.br/download-de-dados/servidores)

Os dados de origem são segmentados em **subsistemas** (SIAPE, BACEN, Militares, DEFESA, Reserva/Reforma). Cada tabela do conjunto combina um ou mais desses subsistemas (ver seção *Tabelas e Subsistemas*).

---

## Particularidades da Fonte (pontos críticos)

Estas duas características do Portal da Transparência foram a causa raiz da defasagem que travou a atualização das tabelas. Ambas estão tratadas na função `source_url_is_available` (`pipelines/crawler/cgu/utils.py`).

### 1. Bloqueio por falta de User-Agent (HTTP 405)

O portal responde **HTTP 405** a requisições que não enviam um `User-Agent` de navegador. Antes do fix, o gate de verificação usava `requests.get(url)` sem cabeçalho e interpretava o 405 como "URL inexistente", encerrando o flow e impedindo a ingestão.

**Tratamento:** todas as requisições (gate e download) enviam `User-Agent` de Chrome (`constants.BROWSERS_USER_AGENT["chrome"]`).

### 2. Geração assíncrona do ZIP (HTTP 202)

O portal prepara o arquivo de forma assíncrona: responde **HTTP 202** enquanto gera o ZIP e **200** (via redirect) quando o arquivo está pronto. Uma verificação ingênua por `status_code == 200` falha nesse intervalo.

**Tratamento:** `source_url_is_available` faz *retry* enquanto a resposta for 202 (`max_retries` tentativas com `wait_seconds` de espera) e só considera a URL disponível ao receber 200.

```python
# pipelines/crawler/cgu/utils.py
headers = {"User-Agent": constants.BROWSERS_USER_AGENT.value["chrome"]}
# 200 -> pronto | 202 -> preparando (retry) | outro -> indisponível
```

> **Log de erro que originava a defasagem:** a URL do mês corrente retornava 405, o gate registrava `A URL {url} não existe!` e o flow encerrava sem persistir dados, mês após mês.

---

## Disponibilidade "tudo-ou-nada"

O Portal publica os subsistemas de um mesmo mês em **datas escalonadas**. Uma tabela que combina vários subsistemas (ex.: `observacoes`, com 9 pacotes) só pode ser ingerida quando **todos** os seus subsistemas estiverem disponíveis.

O gate `verify_all_url_exists_to_download` (`pipelines/crawler/cgu/tasks.py`) exige que todas as URLs da tabela estejam disponíveis antes de iniciar o download. Se algum subsistema ainda não foi publicado, o flow encerra sem persistir e tenta novamente no próximo run.

- **Decisão de produto:** mantém-se o comportamento tudo-ou-nada — **não há ingestão parcial de mês**. Um mês só entra quando está completo, evitando materializações incompletas.
- No gate, o *retry* é curto (`max_retries=1`, `wait_seconds=10`), pois o objetivo ali é apenas checar disponibilidade, não aguardar a geração do ZIP.
- Subsistemas indisponíveis no momento do download são registrados em log (`level="warning"`) e pulados, em vez de derrubar o flow silenciosamente.

---

## Estrutura dos Flows

Cada tabela tem um flow próprio (`pipelines/datasets/br_cgu_servidores_executivo_federal/flows.py`), gerado por `_flow_factory(table_id, cron)` e agendado em horários distintos da manhã para escalonar a carga. Todos delegam a `_run_cgu_servidores_publicos` (`pipelines/crawler/cgu/flows.py`).

### Parâmetros de execução

| Parâmetro | Default | Observação |
|-----------|---------|------------|
| `relative_month` | `1` | Mês relativo a baixar; em prod o ponteiro de metadados avança 1 mês por run |
| `target` | `"prod"` | Use `"dev"` para testes |
| `materialize_after_dump` | `True` | Quando `True`, escreve no bucket `basedosdados` (prod) e materializa prod |
| `update_metadata` | `True` | Atualiza ponteiro de metadados no backend |
| `force_run` | `False` | `True` ignora o poll de fonte |

> **Testes em dev sem tocar prod:** rode com `target="dev"`, `materialize_after_dump=False`, `force_run=True` e varie `relative_month` na mão para backfill de teste. Com os defaults (`target="prod"`, `materialize_after_dump=True`), o flow grava no bucket de **produção** mesmo rodando no worker de dev.

---

## Tabelas e Subsistemas

Mapeamento definido em `get_source` (`pipelines/crawler/cgu/utils.py`).

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

- **Destravamento da atualização (PR #1623):** substituição de `requests.get(url)` cru por `source_url_is_available` no gate e no download, tratando 405 (User-Agent) e 202 (ZIP assíncrono). Validado end-to-end em dev nas 7 tabelas (`dbt run` + `dbt test` OK). Mesma abordagem aplicada anteriormente em `br_cgu_beneficios_cidadao`.

> Nota: o mesmo PR também corrigiu, à parte, a defasagem de `br_ibge_pnadc/microdados` (bump de `certifi` para incluir a raiz de certificação usada pelo `ftp.ibge.gov.br`). Aquela mudança não pertence a este conjunto e está documentada no próprio PR.
