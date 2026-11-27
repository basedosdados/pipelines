# Documentação do Conjunto de Dados: br_bndes_fundos_garantidores (Fundos Garantidores)

Operações de crédito com garantia dos fundos garantidores do BNDES, com uma tabela por
fundo ou programa. Cada linha é uma operação.

- Fonte: [Central de Downloads do BNDES](https://www.bndes.gov.br/wps/portal/site/home/transparencia/centraldedownloads)
- Código: `pipelines/datasets/br_bndes_fundos_garantidores/` e
  `models/br_bndes_fundos_garantidores/`

## A fonte

A Central de Downloads é uma página com vários itens, cada um com um arquivo XLSX e um
CSV de mesmo conteúdo. A lista vem em blocos JSON dentro da página, com os campos
`nome`, `titulo` e `urlArquivo2` (CSV).

| item (`nome`) | tabela | situação |
|---|---|---|
| `operacoes-do-fgi-tradicional` | `operacoes_fgi_tradicional` | ativo |
| `operacoes-do-peac-fgi-2022` | `operacoes_fgi_peac_2022` | ativo |
| `operacoes-fundo-garantidor-bndes-sebrae` | `operacoes_fg_sebrae` | ativo |
| `operacoes-do-fgenergia` | `operacoes_fgenergia` | operações só de 2023 |
| `operacoes-do-fgi-peac-2020` | `operacoes_fgi_peac_2020` | encerrado (posição 31/12/2020) |
| `operacoes-do-peac-fgi-cs-rs-2023-2024-set24` | `operacoes_fgi_peac_cs_rs` | encerrado (posição 31/12/2024) |

O título traz a **posição**, a data de referência da carteira (ex.: "posição em
31.10.2026"). A cada nova posição, o item é republicado com o histórico completo do
fundo.

### Particularidades da fonte

- **O endereço do arquivo muda a cada posição.** O código localiza o item pelo `nome`.
- **O título já ficou atrasado em relação ao arquivo.** O código lê a posição do título
  e do nome do arquivo e usa a mais recente.
- **Não há `Last-Modified`.** O código no fim da URL (`CACHEID=...-<código>`) codifica
  o momento do upload, em milissegundos e base64 com o alfabeto `0-9a-zA-Z-_`.
- **A publicação não tem dia fixo.** As posições recentes saíram entre os dias 3 e 15
  do mês seguinte.
- **O site recusa os IPs do cluster.** As requisições passam por `brasil_proxy_url()`.
- **Não há identificador da operação.** Com o CNPJ mascarado, operações do mesmo
  cliente, no mesmo dia, com o mesmo valor e agente ficam idênticas. As linhas
  idênticas (0,1% a 1,3% por tabela; nenhuma no FGEnergia) se concentram em bancos
  de montadoras, como na compra de frota com um financiamento por veículo (87% das
  da Tradicional), e em valores redondos repetidos para o mesmo cliente. São
  mantidas como publicadas, e por isso não há teste de unicidade.

Dois arquivos fogem do layout padrão (UTF-8, `;`, decimal em vírgula, datas
`aaaa-mm-dd`):

| fundo | diferença |
|---|---|
| PEAC 2020 | cabeçalho em latin-1 e dados em cp850; colunas por extenso; data `d/m/aaaa`; porte em maiúsculas sem acento; sem valor desembolsado |
| PEAC CS-RS | coluna `cnpj_cpf_cliente`; decimal em ponto |

## As tabelas

Uma linha por operação. As tabelas têm as mesmas colunas, com duas exceções: a
`operacoes_fgi_peac_2020` não tem `valor_desembolsado`, e só a
`operacoes_fgi_tradicional`, que tem CPF, usa `cpf_cnpj_cliente`; as demais usam
`cnpj_cliente`.

| coluna | conteúdo |
|---|---|
| `ano` | ano da solicitação de outorga; partição |
| `data_solicitacao_outorga` | data da solicitação da garantia |
| `sigla_uf`, `id_municipio` | UF e município do investimento |
| `sigla_uf_sede_cliente`, `id_municipio_sede_cliente` | UF e município da sede do cliente |
| `cpf_cnpj_cliente` ou `cnpj_cliente` | documento do cliente, mascarado pela fonte |
| `nome_cliente`, `porte_cliente` | nome e porte do cliente |
| `valor_operacao`, `valor_garantido`, `valor_desembolsado` | valores em reais |
| `nome_agente_financeiro` | agente financeiro da operação |

A limpeza (`utils.py`) é a mesma para todos os fundos; as diferenças de cada arquivo
ficam em `constants.LAYOUT`:

```python
"operacoes_fgi_peac_cs_rs": {
    "colunas": {"cnpj_cpf_cliente": "cnpj_cpf_cliente_mascarado"},  # fonte -> padrão
    "decimal": ".",
},
```

`transformar` normaliza espaços e `&AMP;`, padroniza o porte da PEAC 2020, confere o
formato dos valores antes de trocar o decimal e grava as datas em `aaaa-mm-dd`. Nenhuma
linha é removida, e `limpar` para com erro se o total gravado diferir do arquivo.

O código IBGE do município é obtido no dbt pela macro `fundos_garantidores_municipio`
(`macros/br_bndes_fundos_garantidores_municipio.sql`): o diretório de municípios da BD
mais uma lista de grafias da fonte que não casam com ele (ex.: `AUGUSTO SEVERO` para
Campo Grande/RN). Uma grafia nova deixa `id_municipio` vazio, e o teste só acusa
quando isso passa de 1% das linhas; a correção é acrescentá-la à lista.

Cada tabela tem o `not_null_proportion_multiple_columns` com `at_least: 0.99` e o
`relationships` das colunas de diretório. Duas colunas ficam fora da proporção,
porque a fonte publica vazios: `nome_cliente` na PEAC CS-RS (683 linhas, 10,6%) e
`porte_cliente` no FG Sebrae (302 linhas, 1,25%).

## Como o flow funciona

Só os fundos ativos têm flow: Tradicional, PEAC 2022 e FG Sebrae. Cada um segue o
pipeline em estágios (`pipelines/utils/stage_dispatch.py`):

1. **`check_update`**: `buscar_item_central` lê a posição na Central, e o pipeline
   compara o primeiro dia do mês da posição com o fim da cobertura da tabela. Se a
   fonte não estiver à frente, termina com `Não há novas atualizações na fonte
   original`.
2. **`extract_and_load`**: baixa o CSV, aplica `limpar` e substitui a staging em
   `basedosdados-dev` (`dump_mode="overwrite"`).
3. **`build_and_promote`**: roda e testa o dbt e, em produção, atualiza a tabela e a
   cobertura.

Com posição em 31/10/2026, a fonte vale 2026-10-01 e a cobertura vai até 2026-10-30:
sem novidade. A posição de novembro passa a valer 2026-11-01.

**Execução verde não quer dizer que ingeriu.** Para saber se entrou dado, procure no
log o disparo do `extract_and_load` ou veja se a cobertura andou.

## Agendamento

Toda segunda-feira, no horário de Brasília: Tradicional às 2h40, PEAC 2022 às 2h45 e
FG Sebrae às 2h55 (`constants.CHECK_UPDATE_CRON`). Como a publicação não tem dia fixo,
a nova posição entra em até uma semana.

## Parâmetros

O `check_update` não recebe parâmetros. O `extract_and_load` recebe os que o
`check_update` repassa:

| parâmetro | conteúdo |
|---|---|
| `reference_date` | primeiro dia do mês da posição |
| `posicao` | data da posição |

O download não depende deles: sempre baixa o arquivo atual.

## Rodar manualmente e reprocessar

No pool de dev (label `deploy-flow` na PR), o `check_update` roda sem parâmetros. Para
forçar o download:

```json
{"download_params": {"reference_date": "2026-10-01", "posicao": "2026-10-31"}}
```

O `check_update` grava a verificação da fonte no backend de produção, mesmo no pool de
dev.

## Tabelas sem flow

FGEnergia, PEAC 2020 e PEAC CS-RS foram carregados uma vez. Se o BNDES substituir um
desses arquivos, recarregue com os scripts de `models/br_bndes_fundos_garantidores/code/`:

```bash
uv run python models/br_bndes_fundos_garantidores/code/limpar.py operacoes_fgi_peac_2020
uv run python models/br_bndes_fundos_garantidores/code/upload.py operacoes_fgi_peac_2020
```

Os dados ficam em `~/Downloads/br_bndes_fundos_garantidores_data/` (variável
`BNDES_FGI_DATA`).
