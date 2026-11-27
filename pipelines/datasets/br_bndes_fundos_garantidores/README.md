# Documentação do Conjunto de Dados: br_bndes_fundos_garantidores (Fundos Garantidores)

Operações de crédito com garantia dos fundos garantidores do BNDES. Os fundos cobrem
parte do risco do crédito que os agentes financeiros concedem, principalmente a micro,
pequenas e médias empresas. Há uma tabela por fundo ou programa, e cada linha é uma
operação.

- Fonte: [Central de Downloads do BNDES](https://www.bndes.gov.br/wps/portal/site/home/transparencia/centraldedownloads)
- Código: `pipelines/datasets/br_bndes_fundos_garantidores/` e
  `models/br_bndes_fundos_garantidores/`

## A fonte

A Central de Downloads é uma página única com vários itens. Cada item tem um título,
um arquivo XLSX e um arquivo CSV com o mesmo conteúdo. A lista de itens não está no
HTML visível: ela vem em blocos JSON dentro da página, com os campos `nome`, `titulo`,
`urlArquivo1` (XLSX) e `urlArquivo2` (CSV).

| item (`nome`) | tabela | situação |
|---|---|---|
| `operacoes-do-fgi-tradicional` | `operacoes_fgi_tradicional` | ativo, posição mensal |
| `operacoes-do-peac-fgi-2022` | `operacoes_fgi_peac_2022` | ativo, posição mensal |
| `operacoes-fundo-garantidor-bndes-sebrae` | `operacoes_fg_sebrae` | ativo, posição mensal |
| `operacoes-do-fgenergia` | `operacoes_fgenergia` | posição mensal, operações só de 2023 |
| `operacoes-do-fgi-peac-2020` | `operacoes_fgi_peac_2020` | encerrado, posição fixa em 31/12/2020 |
| `operacoes-do-peac-fgi-cs-rs-2023-2024-set24` | `operacoes_fgi_peac_cs_rs` | encerrado, posição fixa em 31/12/2024 |

O título traz a **posição**, a data de referência da carteira: "Operações com garantia
do FGI Tradicional (posição em 31.10.2026)". A posição não é a data da última
operação nem a data do arquivo. O FGEnergia, por exemplo, tem posição em 2026 e
operações só até 2023. A cada nova posição, o BNDES republica o histórico completo do
fundo no mesmo item.

### Particularidades da fonte

- **O endereço do arquivo muda a cada posição.** Por isso o código localiza o item
  pelo `nome`, que é estável, e lê a URL atual a cada execução.
- **O título já ficou atrasado em relação ao arquivo.** Em duas ocasiões, o título
  mostrava uma posição e o nome do arquivo (por exemplo,
  `Central_de_Downloads_-_FGE_-_2026-10[1].csv`) indicava o mês seguinte. O código lê
  as duas datas e usa a mais recente.
- **Não há `Last-Modified`.** As respostas trazem só `ETag`. O fim da URL
  (`CACHEID=...-<código>`) é um identificador de versão que codifica o momento do
  upload em milissegundos, em base64 com o alfabeto `0-9a-zA-Z-_`. O pipeline não usa
  esse dado, mas ele permite saber quando um arquivo foi substituído.
- **A publicação não tem dia fixo.** As posições recentes foram publicadas entre os
  dias 3 e 15 do mês seguinte, e os três fundos ativos saem juntos.
- **O site recusa os IPs do cluster.** As requisições passam pelo proxy brasileiro
  (`brasil_proxy_url()`), como nos outros pipelines do BNDES.
- **Não há identificador da operação** e há linhas idênticas em todos os fundos menos
  o FGEnergia. As linhas são mantidas como publicadas.

### Diferenças entre os arquivos

Quatro fundos seguem o mesmo cabeçalho, em UTF-8, separado por `;`, com decimal em
vírgula e datas `aaaa-mm-dd`. Dois fogem dele:

| fundo | diferença |
|---|---|
| PEAC 2020 | cabeçalho em latin-1 e dados em cp850; nomes de coluna por extenso e em outra ordem; data `d/m/aaaa`; porte em maiúsculas sem acento; sem coluna de valor desembolsado |
| PEAC CS-RS | coluna `cnpj_cpf_cliente` em vez de `cnpj_cpf_cliente_mascarado`; decimal em ponto |

## As tabelas

Cada tabela tem uma linha por operação, com as mesmas colunas, salvo duas exceções:
a `operacoes_fgi_peac_2020` não tem `valor_desembolsado`, porque o arquivo não tem
essa coluna, e só a `operacoes_fgi_tradicional` tem CPF, por isso só ela usa
`cpf_cnpj_cliente`. As demais usam `cnpj_cliente`, como em
`br_bndes_operacoes_contratadas`.

| coluna | conteúdo |
|---|---|
| `ano` | ano da solicitação de outorga; partição |
| `data_solicitacao_outorga` | data em que o agente financeiro solicitou a garantia |
| `sigla_uf`, `id_municipio` | UF e município do investimento |
| `sigla_uf_sede_cliente`, `id_municipio_sede_cliente` | UF e município da sede do cliente |
| `cpf_cnpj_cliente` ou `cnpj_cliente` | documento do cliente, mascarado pela fonte |
| `nome_cliente`, `porte_cliente` | nome e porte do cliente |
| `valor_operacao`, `valor_garantido`, `valor_desembolsado` | valores da operação, em reais |
| `nome_agente_financeiro` | agente financeiro que contratou a operação |

A arquitetura de cada tabela está em
`models/br_bndes_fundos_garantidores/code/architecture/`.

### Tratamentos

A limpeza (`utils.py`) é a mesma para todos os fundos. O que muda de um arquivo para
outro fica em `constants.LAYOUT`, que sobrescreve o padrão de `constants.LAYOUT_PADRAO`:

```python
"operacoes_fgi_peac_cs_rs": {
    "colunas": {"cnpj_cpf_cliente": "cnpj_cpf_cliente_mascarado"},  # nome na fonte -> padrão
    "decimal": ".",
},
```

`ler_csv` lê o arquivo como texto, sem converter vazios. `transformar` leva o arquivo ao
cabeçalho padrão e então:

- troca quebras de linha, espaço não separável e espaços repetidos por um espaço, e
  `&AMP;` por `&`;
- padroniza o porte da PEAC 2020 (`MEDIA` vira `Média`);
- confere o formato dos valores antes de trocar o separador decimal; um valor com
  separador de milhar ou com o decimal errado interrompe a execução;
- grava as datas em `aaaa-mm-dd` e deriva `ano`.

Nenhuma linha é removida: `limpar` compara as linhas gravadas com as do arquivo e para
com erro se diferirem. A staging é toda em texto, com uma pasta por ano
(`ano=aaaa/data.parquet`).

O arquivo traz só o nome do município. O código IBGE é obtido no dbt, pela macro
`fundos_garantidores_municipio` (`macros/br_bndes_fundos_garantidores_municipio.sql`),
que junta o diretório de municípios da BD a uma lista de grafias da fonte que não
casam com ele (nomes antigos, como `AUGUSTO SEVERO` para Campo Grande/RN, e variações,
como `PARATI`). Os dois lados são comparados sem acento, apóstrofo ou hífen. Uma grafia
nova faz o teste `not_null` de `id_municipio` falhar; a correção é acrescentar o par
UF e nome à lista da macro.

## Como os flows funcionam

Só os três fundos ativos têm flows: Tradicional, PEAC 2022 e FG Sebrae. Os demais
foram carregados uma vez. Cada fundo ativo segue o pipeline em estágios
(`pipelines/utils/stage_dispatch.py`):

1. **`check_update`**: `buscar_item_central` lê a página, encontra o item pelo `nome`
   e extrai a posição. `get_latest_update` devolve o primeiro dia do mês da posição, e
   o pipeline compara essa data com o fim da cobertura da tabela no backend. Se a
   posição não estiver à frente, o flow termina com `Não há novas atualizações na
   fonte original`.
2. **`extract_and_load`**: baixa o CSV atual, aplica `limpar` e envia a staging para
   `basedosdados-dev` com `dump_mode="overwrite"`, já que cada posição traz o histórico
   completo.
3. **`build_and_promote`**: o flow genérico roda e testa o dbt em dev e, a partir do
   deployment de produção, leva a tabela para produção e atualiza a cobertura.

A comparação usa o mês porque a cobertura termina dentro do mês da posição: com
posição em 31/10/2026, a fonte vale 2026-10-01 e a cobertura vai até 2026-10-30, então
não há novidade. Quando sair a posição de novembro, a fonte passa a valer 2026-11-01.

**Execução verde não quer dizer que ingeriu.** A maior parte das execuções termina na
comparação do passo 1. Para saber se entrou dado, procure no log o disparo do
`extract_and_load` ou veja se a cobertura andou.

## Agendamento

Toda segunda-feira, no horário de Brasília, um minuto por fundo: Tradicional às 2h40,
PEAC 2022 às 2h45 e FG Sebrae às 2h55 (`constants.CHECK_UPDATE_CRON`). Como a
publicação não tem dia fixo, a verificação semanal faz a nova posição entrar em até uma
semana.

## Testar no pool de dev

Com a label `deploy-flow` na PR, os deployments `dev-` são registrados sem agendamento.

- **`check_update`**: roda sem parâmetros. Com a tabela em dia, termina na
  comparação. Ele grava a verificação da fonte no backend de produção.
- **download (`extract_and_load`)**: dispara o download direto, com os parâmetros que o
  `check_update` passaria:

```json
{"download_params": {"reference_date": "2026-10-01", "posicao": "2026-10-31"}}
```

O download não depende desses valores: ele sempre baixa o arquivo atual. O
`build_and_promote` disparado em seguida usa os modelos dbt da `main`.

## Carga inicial e fundos sem flow

Os scripts em `models/br_bndes_fundos_garantidores/code/` reaproveitam as funções do
pipeline:

```bash
# baixa e limpa os fundos (todos, ou os passados como argumento)
uv run python models/br_bndes_fundos_garantidores/code/limpar.py operacoes_fgi_peac_2020
# recria a staging em basedosdados-dev e confere as linhas
uv run python models/br_bndes_fundos_garantidores/code/upload.py operacoes_fgi_peac_2020
```

É o caminho para atualizar o FGEnergia, a PEAC 2020 ou a PEAC CS-RS se o BNDES
substituir o arquivo. Por padrão, os dados ficam em
`~/Downloads/br_bndes_fundos_garantidores_data/` (variável `BNDES_FGI_DATA`).
