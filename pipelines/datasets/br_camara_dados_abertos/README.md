# Documentação do Conjunto de Dados: br_camara_dados_abertos (Dados Abertos da Câmara dos Deputados)

Dados do portal de Dados Abertos da Câmara dos Deputados: votações, deputados,
proposições, órgãos, eventos, frentes parlamentares, funcionários, licitações e
despesas da cota parlamentar. O conjunto tem 28 tabelas com modelo dbt; 26 delas são
atualizadas por flows diários.

- Fonte: arquivos em bloco do [Dados Abertos da Câmara](https://dadosabertos.camara.leg.br/)
  (`dadosabertos.camara.leg.br/arquivos/<recurso>/csv/`) e, para as despesas, os
  arquivos da cota parlamentar (`www.camara.leg.br/cotas/Ano-<ano>.csv.zip`)
- Código: `pipelines/datasets/br_camara_dados_abertos/` (`flows.py`, `tasks.py`,
  `utils.py` e `constants.py`) e `models/br_camara_dados_abertos/`

## A fonte

O flow usa os arquivos em bloco do portal, e não a API. São arquivos CSV separados por
`;`, de três tipos:

- **um arquivo por ano**: proposições, votações, eventos e licitações
  (`proposicoes-2026.csv`, `votacoes-2026.csv`…) e as despesas
  (`Ano-2026.csv.zip`);
- **um arquivo único**, com o cadastro completo: deputados, ocupações, profissões,
  órgãos, frentes e funcionários (`deputados.csv`, `orgaos.csv`…);
- **um arquivo por legislatura**: os integrantes dos órgãos
  (`orgaosDeputados-L57.csv`).

A Câmara continua alterando os arquivos de anos já encerrados. Por isso o flow baixa,
a cada execução, o arquivo do ano atual **e** o do ano anterior (decisão registrada no
PR #970).

## As tabelas por tema

| tema | tabelas | arquivos da Câmara |
|---|---|---|
| Votação | `votacao`, `votacao_objeto`, `votacao_orientacao_bancada`, `votacao_parlamentar`, `votacao_proposicao` | `votacoes`, `votacoesObjetos`, `votacoesOrientacoes`, `votacoesVotos`, `votacoesProposicoes` (por ano) |
| Deputado | `deputado`, `deputado_ocupacao`, `deputado_profissao` | `deputados`, `deputadosOcupacoes`, `deputadosProfissoes` (únicos) |
| Proposição | `proposicao_microdados`, `proposicao_autor`, `proposicao_tema` | `proposicoes`, `proposicoesAutores`, `proposicoesTemas` (por ano) |
| Órgão | `orgao`, `orgao_deputado` | `orgaos` (único), `orgaosDeputados-L57` (57ª legislatura) |
| Evento | `evento`, `evento_orgao`, `evento_presenca_deputado`, `evento_requerimento` | `eventos`, `eventosOrgaos`, `eventosPresencaDeputados`, `eventosRequerimentos` (por ano) |
| Funcionário | `funcionario` | `funcionarios` (único) |
| Frente | `frente`, `frente_deputado` | `frentes`, `frentesDeputados` (únicos) |
| Licitação | `licitacao`, `licitacao_proposta`, `licitacao_contrato`, `licitacao_item`, `licitacao_pedido` | `licitacoes`, `licitacoesPropostas`, `licitacoesContratos`, `licitacoesItens`, `licitacoesPedidos` (por ano) |
| Despesa | `despesa` | `Ano-<ano>.csv.zip` (cota parlamentar, por ano) |
| Legislatura | `legislatura`, `legislatura_mesa` | sem flow (ver "Tabelas sem flow e itens legados") |

### Como as tabelas se relacionam

As tabelas se ligam por identificadores da própria Câmara:

| chave | tabela de referência | aparece em |
|---|---|---|
| `id_deputado` | `deputado` | `deputado_ocupacao`, `deputado_profissao`, `votacao_parlamentar`, `proposicao_autor`, `evento_presenca_deputado`, `frente_deputado`, `despesa`, `legislatura_mesa` |
| `id_votacao` | `votacao` | `votacao_objeto`, `votacao_orientacao_bancada`, `votacao_parlamentar`, `votacao_proposicao` |
| `id_proposicao` | `proposicao_microdados` | `proposicao_autor`, `proposicao_tema`, `votacao_objeto`, `votacao_proposicao`, `evento_requerimento` |
| `id_evento` | `evento` | `evento_orgao`, `evento_presenca_deputado`, `evento_requerimento`, `votacao` |
| `id_orgao` | `orgao` | `orgao_deputado`, `evento_orgao`, `votacao`, `licitacao_pedido`, `legislatura_mesa` |
| `id_licitacao` | `licitacao` | `licitacao_proposta`, `licitacao_contrato`, `licitacao_item`, `licitacao_pedido` |
| `id_frente` | `frente` | `frente_deputado` |
| `id_legislatura` | `legislatura` | `votacao_parlamentar`, `frente`, `despesa`, `legislatura_mesa`; em `deputado`, como `id_inicial_legislatura` e `id_final_legislatura` |

Duas exceções: `orgao_deputado` identifica o deputado só pelo nome
(`nome_deputado`), sem `id_deputado`, e `funcionario` não tem chave em comum com as
outras tabelas.

## Como o flow funciona

Há um flow por tabela (`br_camara_dados_abertos__<tabela>`), todos criados pela mesma
função, `_camara_flow`, em `flows.py`. Cada execução:

1. **Verifica o arquivo do ano**: `check_if_url_is_valid` consulta o endereço do
   arquivo do ano atual.
   - Se ele existe, o flow segue.
   - Se só o do ano anterior existe, o flow termina sem baixar nada. É o que acontece
     no começo de janeiro, antes de a Câmara publicar o arquivo do ano novo.
   - Se nenhum dos dois existe, a execução para com erro.
2. **Baixa o ano atual e o anterior**: `save_data` chama `download_and_read_data`,
   que baixa os dois arquivos. Nas tabelas de arquivo único, o mesmo arquivo é
   baixado duas vezes, e o resultado é um só.
3. **Trata e grava um CSV por ano**: `save_data` aplica os tratamentos da tabela e
   grava cada ano em um arquivo com nome fixo (por exemplo,
   `votacoes_2026.csv`). Os tratamentos removem `;` e quebras de linha dos campos de
   texto livre (ementas, descrições, observações), renomeiam colunas aninhadas de
   `evento` e `frente_deputado` e, em `proposicao_microdados`, preenchem o `ano` a
   partir da data de apresentação quando a fonte traz `0`.
4. **Carrega em dev e depois em produção**: os CSVs vão para o bucket
   `basedosdados-dev` e o dbt roda e testa em dev; só então vão para o bucket
   `basedosdados` e o dbt roda e testa em produção. Uma falha em dev impede a ida para
   produção. Como o nome do arquivo de cada ano é fixo, reenviar um ano substitui o
   arquivo anterior; os anos mais antigos, enviados em execuções passadas, continuam na
   staging.
5. **Atualiza a cobertura**: `register_table_materialization_task` registra a
   cobertura da tabela no site.

**Não há verificação de novidade.** Diferente de outros conjuntos, este não compara a
fonte com a cobertura da tabela nem registra a publicação da fonte: todo dia, cada
flow baixa os arquivos e reconstrói a tabela. Todas as tabelas são materializadas
como `table`, ou seja, o dbt recria a tabela inteira a partir de todos os arquivos da
staging. Uma execução verde quer dizer que a tabela foi reconstruída, e não
necessariamente que a fonte tinha dados novos.

### Cobertura e BD Pro

A cobertura depende da tabela (`update_metadata_variable_dictionary`, em
`constants.py`):

- **Tabelas com uma coluna de data**: os 6 meses mais recentes ficam restritos ao BD
  Pro, e o restante é aberto. O corte é recalculado a cada execução, e as regras de
  acesso no BigQuery são recriadas.

  | tabela | coluna de data |
  |---|---|
  | `votacao`, `votacao_objeto`, `votacao_parlamentar`, `votacao_proposicao`, `proposicao_microdados` | `data` |
  | `evento`, `evento_presenca_deputado`, `orgao`, `orgao_deputado` | `data_inicio` |
  | `frente` | `data_criacao` |
  | `funcionario` | `data_inicio_historico` |
  | `licitacao` | `data_autorizacao` |
  | `licitacao_contrato` | `data_assinatura` |
  | `licitacao_pedido` | `data_cadastro` |
  | `despesa` | `data_emissao` |

- **Tabelas sem coluna de data confiável** (`deputado`, `deputado_ocupacao`,
  `deputado_profissao`, `evento_orgao`, `evento_requerimento`, `frente_deputado`,
  `licitacao_item`, `licitacao_proposta`, `proposicao_autor`, `proposicao_tema`,
  `votacao_orientacao_bancada`): são inteiramente abertas, e a cobertura registra a
  data da última atualização da tabela.

## Agendamento

Um flow por tabela, todo dia, no horário de Brasília, das 6h às 10h. Os horários
seguem a ordem dos temas, com 10 minutos entre um flow e o seguinte, exceto em torno
de `licitacao` (9h15), que fica a 5 minutos de `frente_deputado` e de
`licitacao_proposta`:

| horário | tabelas |
|---|---|
| 6h00 a 6h40 | `votacao`, `votacao_objeto`, `votacao_orientacao_bancada`, `votacao_parlamentar`, `votacao_proposicao` |
| 6h50 a 7h10 | `deputado`, `deputado_ocupacao`, `deputado_profissao` |
| 7h20 a 7h40 | `proposicao_microdados`, `proposicao_autor`, `proposicao_tema` |
| 7h50 e 8h00 | `orgao`, `orgao_deputado` |
| 8h10 a 8h40 | `evento`, `evento_orgao`, `evento_presenca_deputado`, `evento_requerimento` |
| 8h50 | `funcionario` |
| 9h00 e 9h10 | `frente`, `frente_deputado` |
| 9h15 a 9h50 | `licitacao` (9h15), `licitacao_proposta`, `licitacao_contrato`, `licitacao_item`, `licitacao_pedido` |
| 10h00 | `despesa` |

## Parâmetros

| parâmetro | padrão | quando mudar |
|---|---|---|
| `materialize_after_dump` | `True` | `False` para parar depois do dbt em dev, sem levar nada para produção |
| `update_metadata` | `True` | `False` para não registrar cobertura |
| `target` | `prod` | define onde roda o segundo `dbt run`; o upload e o registro de cobertura continuam indo para produção |
| `force_run` | `False` | `True` para seguir mesmo quando só existe o arquivo do ano anterior (passo 1) |

Como o download sempre pede o arquivo do ano atual, `force_run=True` não permite
processar só o ano anterior: sem o arquivo do ano atual, a execução para com erro no
download.

Para testar no pool de dev sem gravar nada em produção:

```json
{"materialize_after_dump": false, "update_metadata": false}
```

## Rodar manualmente e reprocessar

Como cada execução reconstrói a tabela inteira, **reprocessar uma tabela é rodar o
flow dela** com os parâmetros padrão. Os dois anos mais recentes são baixados de novo,
e os anos anteriores vêm dos arquivos que já estão na staging.

O flow só baixa os dois anos mais recentes. Os anos anteriores que estão na staging
vieram de execuções passadas e da carga histórica inicial; recarregar um deles fica
fora do flow.

## Tabelas sem flow e itens legados

- **`legislatura` e `legislatura_mesa`** têm modelo dbt, mas nenhum flow as atualiza.
  A última atualização foi em 26/01/2024, e a `legislatura_mesa` registra as Mesas até
  31/01/2023.
- **`orgao_deputado`** lê o arquivo da 57ª legislatura (`orgaosDeputados-L57.csv`),
  fixo em `constants.py`. Essa legislatura termina em 31/01/2027; a partir daí, a
  tabela só recebe a legislatura nova se o endereço for alterado.
- **`models/br_camara_dados_abertos/code/code.ipynb`** é o notebook da carga
  histórica inicial, com os anos até 2022, feito uma única vez a partir de planilhas
  locais. Não faz parte do flow.
- **`sigla_partido`** está publicada no BigQuery desde 2021, mas não tem modelo nem
  flow neste repositório.
