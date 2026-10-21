# Documentação do Conjunto de Dados: br_anp_precos_combustiveis (Preços de Combustíveis)

Preços de venda de combustíveis ao consumidor, coletados semanalmente pela Agência
Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) em uma amostra de postos
revendedores. Cada linha é o preço de um produto, em um posto, em uma data de coleta.

- Fonte: [Série Histórica de Preços de Combustíveis e de GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis),
  arquivos "últimas 4 semanas"
- Código: `pipelines/datasets/br_anp_precos_combustiveis/flows.py`,
  `pipelines/crawler/anp_precos_combustiveis/` e `models/br_anp_precos_combustiveis/`

## A fonte

A ANP publica três arquivos CSV em endereços fixos, substituídos a cada semana:

| arquivo | produtos |
|---|---|
| `ultimas-4-semanas-glp.csv` | GLP (botijão de 13 kg) |
| `ultimas-4-semanas-gasolina-etanol.csv` | gasolina, gasolina aditivada e etanol |
| `ultimas-4-semanas-diesel-gnv.csv` | diesel, diesel S10 e GNV |

Os arquivos são separados por `;`, em UTF-8, com decimal em vírgula e datas no formato
`dd/mm/aaaa`. Cada um traz **só as últimas 4 semanas** de coleta: a cada semana entra
a mais recente e sai a mais antiga. A tabela acumula as semanas desde 2004; os
arquivos, não.

A mesma página publica a série histórica em arquivos semestrais (por exemplo,
`ca-2026-01.zip`), que o flow não lê.

### Particularidades da fonte

- **A pesquisa é amostral.** Não cobre todos os postos nem todos os municípios: nas
  4 semanas até 02/10/2026, os arquivos têm 9.173 postos em 415 municípios. Um
  município fora da amostra não aparece na tabela.
- **As coletas se concentram de segunda a quinta-feira.** Sexta-feira é rara, e sábado
  quase não aparece.
- **A coluna "Valor de Compra" vem vazia** nos arquivos atuais, então `preco_compra`
  não é preenchido nas semanas recentes.
- **O arquivo não traz o código IBGE do município**, só o nome e a UF.
- **O formato servido já mudou.** Em maio de 2026, a ANP passou a servir planilhas
  XLSX nos endereços `.csv` e depois voltou ao CSV. O flow lê CSV.

## A tabela `microdados`

Uma linha por produto, posto e data de coleta.

| coluna | conteúdo |
|---|---|
| `ano` | ano da coleta |
| `sigla_uf` | UF do posto |
| `id_municipio` | código IBGE do município, com 7 dígitos |
| `bairro_revenda`, `cep_revenda`, `endereco_revenda` | endereço do posto |
| `cnpj_revenda` | CNPJ do posto, sem pontuação |
| `nome_estabelecimento`, `bandeira_revenda` | nome e bandeira do posto |
| `data_coleta` | data da coleta do preço |
| `produto` | combustível |
| `unidade_medida` | `R$/litro`, `R$/m3` (GNV) ou `R$/13kg` (GLP) |
| `preco_compra` | preço pago pelo posto à distribuidora |
| `preco_venda` | preço ao consumidor |

O crawler (`download_and_transform`) faz os tratamentos que dependem do arquivo:

- **`id_municipio`**: o arquivo não traz o código, então o crawler cruza o nome do
  município e a UF com `br_bd_diretorios_brasil.municipio`, com os nomes em maiúsculas
  e sem acento. Dois nomes do diretório são ajustados para coincidir com a grafia da
  ANP: `ESPIGAO D'OESTE` e `SANT'ANA DO LIVRAMENTO`.
- **`endereco_revenda`** junta rua, número e complemento, separados por vírgula.
- **`data_coleta`** passa para `aaaa-mm-dd`, e `ano` sai dela.
- Os preços trocam a vírgula decimal por ponto, o CEP perde o hífen e as unidades de
  medida são padronizadas. A região (`Regiao - Sigla`) é descartada.

O modelo dbt converte os tipos, tira a pontuação do CNPJ e deixa os textos com a
primeira letra maiúscula (`initcap`). Por isso os produtos aparecem como `Gasolina`,
`Gasolina Aditivada`, `Etanol`, `Diesel`, `Diesel S10`, `Glp` e `Gnv`.

A tabela é **incremental**: cada `dbt run` só acrescenta as datas de coleta posteriores
à última que já está na tabela. Como cada execução reenvia as 4 semanas do arquivo,
as datas que a tabela já tem são ignoradas e nada é duplicado.

Na staging, há uma pasta por data de coleta (`data_coleta=aaaa-mm-dd/data.csv`). Na
tabela final, a partição é por `ano`.

## Como o flow funciona

Há um único flow, `br_anp_precos_combustiveis__microdados`. Cada execução:

1. **Descobre a data mais recente da fonte**: `get_data_source_anp_max_date` baixa o
   arquivo de GLP e pega a maior data de coleta. O arquivo de GLP representa os três,
   que trazem as mesmas datas.
2. **Verifica se há novidade**: `poll_source_for_update_task` compara essa data com o
   fim da cobertura da tabela em produção, no formato `%Y-%m-%d`. Se a fonte não
   estiver à frente, o flow termina sem baixar os três arquivos para carga (só o de
   GLP, usado no passo 1, já foi baixado).
3. **Registra a publicação da fonte**: `commit_source_update_task` grava o `Update` da
   fonte **antes do download**. Isso não trava a próxima tentativa, porque a
   verificação do passo 2 olha a cobertura da tabela, não esse registro. Se o flow
   falhar depois, a cobertura não anda e a execução seguinte tenta de novo.
4. **Baixa e trata os três arquivos**: `download_and_transform` baixa as 4 semanas dos
   três produtos e aplica os tratamentos descritos acima. Se algum download não
   responder com sucesso, a execução para com erro.
5. **Gera as partições**: `make_partitions` grava um CSV por data de coleta.
6. **Carrega em dev e depois em produção**: os CSVs vão para o bucket
   `basedosdados-dev` e o dbt roda e testa em dev; só então vão para o bucket
   `basedosdados` e o dbt roda e testa em produção. Uma falha em dev impede a ida para
   produção.
7. **Atualiza a cobertura**: `register_table_materialization_task` lê a última data de
   coleta da tabela de produção e atualiza a cobertura no site.

A cobertura tem duas partes: as **6 semanas** mais recentes ficam restritas ao BD Pro,
e o restante é aberto. O passo 7 recalcula esse corte a cada semana nova e recria as
regras de acesso no BigQuery, então a janela anda sozinha. Por exemplo, com dados até
02/10/2026, a parte aberta vai até 21/08/2026, e a do BD Pro vai de 22/08 a 02/10.

**Execução verde não quer dizer que ingeriu.** Como a ANP publica uma vez por semana e
o flow roda todo dia, a maior parte das execuções termina com sucesso depois de
conferir a data no arquivo de GLP, sem carregar nada.
Para saber se entrou dado, procure no log as mensagens `Há atualizações na fonte
original` e `dbt run OK`, ou veja se a cobertura andou.

## Agendamento

`0 10 * * *`, no horário de Brasília: todo dia às 10h. A ANP atualiza os arquivos uma
vez por semana, sem dia fixo registrado; a execução diária faz a semana nova entrar no
primeiro dia depois da publicação. Nos outros dias, o flow só confere a data e termina.

## Parâmetros

| parâmetro | padrão | quando mudar |
|---|---|---|
| `materialize_after_dump` | `True` | `False` para parar depois do dbt em dev, sem levar nada para produção |
| `update_metadata` | `True` | `False` para não registrar cobertura nem a publicação da fonte |
| `target` | `prod` | define onde roda o segundo `dbt run`; o upload e o registro de cobertura continuam indo para produção |
| `force_run` | `False` | `True` para pular a verificação de novidade (passo 2) |

Para testar no pool de dev sem gravar nada em produção:

```json
{"materialize_after_dump": false, "update_metadata": false, "force_run": true}
```

O teste funciona em qualquer dia: com `force_run`, o flow sempre baixa e trata as 4
semanas do arquivo, mesmo quando a tabela já está em dia.

## Rodar manualmente e reprocessar

**Semanas atrasadas:** basta rodar o flow com os parâmetros padrão, desde que as
semanas ainda estejam nos arquivos. Todas as datas posteriores à última da tabela
entram na mesma execução. Uma semana que já saiu dos arquivos de 4 semanas não pode
ser recuperada pelo flow; ela só está nos arquivos semestrais da série histórica.

**Uma data que já está na tabela** não é reprocessada pelo flow, nem com `force_run`:
a tabela incremental ignora as datas que já tem. Se a ANP corrigir uma data que ainda
está nos arquivos:

1. rode o flow com `force_run=true`, para que os arquivos corrigidos substituam os
   antigos nas pastas das datas, na staging de dev e na de produção;
2. reconstrua a tabela com `dbt run --select br_anp_precos_combustiveis__microdados
   --full-refresh`, que lê a staging inteira de novo. Em produção, use o deployment
   "BD template: Executa DBT model", com `target="prod"` e `flags="--full-refresh"`;
3. rode o flow mais uma vez com `force_run=true`. A reconstrução remove as regras de
   acesso do BD Pro, e o passo 7 desta execução as recria.
