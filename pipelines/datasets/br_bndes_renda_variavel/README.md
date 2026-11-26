# br_bndes_renda_variavel

Carteira de renda variável do BNDES ao fim de cada ano, desde 2006, e desembolsos via renda variável,
desde 2007, em 4 tabelas publicadas no Portal de Dados Abertos do BNDES (CKAN).

## Fonte

Pacote CKAN [`renda-variavel`](https://dadosabertos.bndes.gov.br/dataset/renda-variavel). Cada tabela
é um CSV com o histórico inteiro, republicado inteiro a cada versão. O CKAN declara atualização
semestral.

- Windows-1252, `;` como separador e vírgula decimal.
- O dicionário de cada CSV é um PDF no próprio pacote e fica na fonte.
- A fonte não informa a data de referência do último ano. A tabela traz o que o BNDES publicou, e a
  próxima versão do arquivo a substitui.
- O portal recusa conexões do cluster: toda chamada passa pelo proxy brasileiro
  (`brasil_proxy_url()`).

## Tabelas

| Tabela | CSV da fonte | Uma linha por |
|---|---|---|
| `empresa_participacao_acionaria` | `renda-variavel-participacoes-acionarias-historico-da-carteira.csv` | ativo e ano |
| `fundo_participacao` | `renda-variavel-fundos-de-investimentos-historico-da-carteira.csv` | fundo e ano |
| `empresa_debenture` | `renda-variavel-debentures-historico-da-carteira.csv` | ativo e ano |
| `desembolso` | `renda-variavel-desembolsos-via-renda-variavel.csv` | operação |

O nome começa pela entidade da tabela (empresa ou fundo). Na `empresa_participacao_acionaria` e na
`fundo_participacao`, a chave é `ano` + `sigla`. Nos arquivos de 13/08/2026, nenhuma sigla aparece com
mais de um CNPJ. Já um mesmo CNPJ pode aparecer com mais de uma sigla quando a empresa troca de nome
(ELETROBRÁS em 2006, AXIA EX ELETROB em 2026), e a AES TH vem sem CNPJ em 2007. A partição é por `ano`. Todas são abertas
(`all_free`): o CKAN declara publicação semestral, abaixo do limite mensal a partir do qual a BD
reserva a janela mais recente ao BD Pro.

## Tratamento

Nenhuma linha é criada, apagada ou somada.

- **CNPJ** fica só com os 14 dígitos, ligado a `br_bd_diretorios_brasil.empresa`; `N/A` vira nulo.
- **Categorias** (`tipo_ativo`, `setor_atividade`, `tipo_capital`, `objetivo_predominante`) ficam com
  inicial maiúscula e o resto minúsculo: `TI - TECNOLOGIA DA INFORMAÇÃO` vira
  `Ti - tecnologia da informação`. `N/A` e `-` viram nulo.
- **Indicadores** `SIM` e `NÃO` viram 1 e 0. Outro valor interrompe a limpeza.
- **Números** trocam a vírgula decimal por ponto, sem arredondar. As participações estão em %, os
  desembolsos em reais nominais.
- **Sigla, razão social e descrição da operação** ficam como a fonte publica. Todo texto perde os
  espaços nas pontas, e célula vazia vira nulo.
- A limpeza para quando a fonte traz uma coluna a mais ou a menos que as de `constants.py`.

## O que a fonte publica e a tabela mantém

Contagens dos arquivos de 13/08/2026 (debêntures de 28/05/2026).

- `empresa_debenture` repete `ano` e `cnpj` em 52 linhas, 26 pares: 19 pares idênticos (ATMA PART,
  ENERGISA, HYPERA, KLABIN) e 7 com duas quantidades no mesmo ano (AGC ENERGIA de 2011 a 2013, KLABIN
  de 2014 a 2017).
  O arquivo não tem identificador de emissão; as linhas ficam como a fonte publica, e a tabela não tem
  chave única.
- `desembolso` tem várias operações por empresa e ano, sem identificador da operação, e também não tem
  chave única.
- `objetivo_predominante` tem duas grafias para reestruturação societária e para FIDC de
  infraestrutura. Ficam como a fonte.
- 23 linhas de `empresa_participacao_acionaria` vêm sem tipo de ativo e sem as três participações.
- O dicionário descreve `-99` como "sem participação ou base acionária indisponível", mas nenhum
  arquivo tem esse valor.
- A razão social muda ao longo dos anos para o mesmo CNPJ (troca de nome, abreviação); a tabela mantém
  a de cada linha.
- Em `fundo_participacao`, `setor_atividade` é `N/A` em todas as linhas, por isso é nula e fica fora
  do teste de proporção de nulos.

## Pipeline

Cada tabela tem um `check_update` e um `extract_and_load`; o `build_and_promote` é o genérico,
compartilhado por todos os conjuntos. Os `check_update` rodam todo dia (America/Sao_Paulo):

| Tabela | Horário |
|---|---|
| `empresa_participacao_acionaria` | 3h10 |
| `fundo_participacao` | 3h15 |
| `empresa_debenture` | 3h20 |
| `desembolso` | 3h35 |

### Detecção de novidade

O `check_update` lê o `last_modified` do recurso no CKAN, uma chamada só ao `resource_show`, e compara
com a "Última atualização na Base dos Dados" (`Table.Update.latest`), via
`compare_against="table_update"`. O `last_modified` muda quando o arquivo é substituído, então uma
correção sem ano novo também dispara a carga.

No primeiro registro de metadados, `Table.Update.latest` precisa ficar antes do `last_modified` do
recurso; do contrário, o primeiro poll não enxerga novidade.

### Carga

O `extract_and_load` baixa o CSV, confere o MD5 com o `hash` do recurso no CKAN, trata as colunas e
grava parquet particionado por `ano`, todo em texto. Como a fonte republica o histórico inteiro, a
staging é reescrita (`dump_mode="overwrite"`) e todas as partições são promovidas.
