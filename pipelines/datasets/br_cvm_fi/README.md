# br_cvm_fi — Documentos de Fundos de Investimento (CVM)

Contexto e particularidades deste conjunto, para quem for dar manutenção nos flows
ou nos modelos.

## Fonte

Portal de dados abertos da CVM. Cada tabela vem de um diretório próprio, e a
cadência de reescrita **não é a mesma em todas**:

| Tabela | Diretório da fonte | Granularidade do arquivo | Reescrita |
|---|---|---|---|
| `documentos_informe_diario` | `FI/DOC/INF_DIARIO/DADOS/` | um zip por competência | diária |
| `documentos_carteiras_fundos_investimento` | `FI/DOC/CDA/DADOS/` | um zip por competência | diária |
| `documentos_informacao_cadastral` | `FI/CAD/DADOS/` | arquivo único (`cad_fi.csv`) | diária |
| `documentos_balancete` | `FI/DOC/BALANCETE/DADOS/` | um zip por competência | mensal |
| `documentos_perfil_mensal` | `FI/DOC/PERFIL_MENSAL/DADOS/` | um csv por competência | mensal |
| `documentos_extratos_informacoes` | `FI/DOC/EXTRATO/DADOS/` | um csv por ano | mensal |

Apesar do nome, o `informe_diario` também é publicado em arquivos mensais — o que é
diário é a reescrita deles.

O mapeamento de colunas aceita `CNPJ_FUNDO` e `CNPJ_FUNDO_CLASSE` para o mesmo
destino (`cnpj`), porque a fonte usa os dois nomes.

## Como o flow escolhe o que baixar

`extract_links_and_dates` lê a listagem HTML do diretório e usa o **mtime** de cada
arquivo, truncado para data. `generate_links_to_download` baixa **só os arquivos cujo
mtime é igual ao máximo**. Não há filtro por competência.

Isso tem duas consequências que já produziram dado faltando:

1. **O mtime não fala sobre a competência.** Se a CVM republicar um mês antigo
   isolado, ele passa a ter o mtime máximo e a run baixa apenas ele, ignorando a
   competência nova, cujo mtime é mais velho. O `max_date` entregue ao poll é o
   mesmo mtime, então a comparação também não fala sobre o dado que falta.
2. **O mtime regride.** A CVM reescreve os arquivos em janelas de cerca de doze
   competências, e arquivos que já estiveram numa janela reaparecem depois com a
   data original. Exemplo verificado: `balancete_fi_202508.zip` foi baixado em
   04/09/2026 como parte do lote de mtime máximo e hoje aparece na listagem com
   mtime de agosto de 2025.

Trocar o critério de mtime para competência é a correção pendente.

## Competência parcial

A CVM publica a competência corrente incompleta e a completa nas reescritas
seguintes, à medida que os fundos entregam. Em 09/09/2026,
`balancete_fi_202608.zip` tinha **3.965 fundos contra 25.444** da competência
anterior — 15% dos fundos e 14% das linhas.

Nada no código detecta isso, então uma competência parcial é ingerida e passa a
constar na cobertura. Ela se cura sozinha: `to_partitions` grava `data.csv` por
partição e o upload é `dump_mode="append"`, então reenviar uma competência já
presente sobrescreve o mesmo caminho, sem duplicar linha.

## Poll e metadados

- O poll compara o mtime máximo da fonte contra o **`Table.Update`**
  (`compare_against="table_update"`), que não avança quando o flow falha — logo uma
  run que morre no meio é retentada na run seguinte em vez de dar verde falso.
- `commit_source_update_task` roda **antes** do download, de propósito: se o flow
  falhar depois, o metadado da fonte ainda registra que havia dado novo publicado.
- Todas as seis tabelas são registradas como `AllBdpro`, então **não há row access
  policy** a reaplicar — `needs_row_access_policy` só vale para `PartBdpro`.
- O `Table.Update` das tabelas está com `entity=week`, embora as competências sejam
  mensais.

## Memória

`_clean_standard_data` salva **uma competência por vez** em vez de concatenar todos
os arquivos baixados antes de gravar. O lote típico é de doze arquivos, e o pico
medido é de 0,65 GiB por arquivo, sem crescer com a quantidade; o `pd.concat` do
lote inteiro chegava a estourar a memória do pod (`OOMKilled`).

Os flows não declaram `job_variables`, então rodam com a memória padrão do work
pool. Se algum dia precisar de mais, é `memory_limit` — a chave `memory` sozinha não
existe no template do pool e é descartada em silêncio.

A saída de `documentos_informacao_cadastral` não é particionada (arquivo único na
fonte), e por isso tem um ramo próprio em `_clean_standard_data`: ali `save_output`
devolve o caminho do `data.csv`, não o do diretório.

## Particionamento dos modelos

Cinco modelos particionam por `ano` com `range` terminando em **2025**, e o dado já
alcança 2026 — tudo de 2026 cai numa partição fora de faixa. A poda por `ano`
continua funcionando, mas o intervalo precisa ser estendido.
`documentos_informacao_cadastral` não é particionado, só clusterizado por
`id_fundo`.

## Estrutura no repositório

Este conjunto está no split legado: as tasks e o corpo do flow vivem em
`pipelines/crawler/cvm/`, e `pipelines/datasets/br_cvm_fi/flows.py` só fabrica os
seis flows com `_cvm_fi_flow`, um por tabela, escalonados de dez em dez minutos a
partir das 17h para não disputarem slot no BigQuery no mesmo instante.

O deploy considera apenas arquivos que definem um objeto `Flow`, ou seja
`flows.py`. Conserto em `tasks.py`, `utils.py` ou `constants.py` **não gera
deployment**: o job de deploy reporta sucesso sem ter deploiado nada, e um trigger
manual roda o código da referência que o deployment já tinha.
