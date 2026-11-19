# Documentação do Conjunto de Dados: br_anp_precos_combustiveis (Preços de Combustíveis)

Preços de venda de combustíveis ao consumidor, coletados semanalmente pela Agência
Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) em uma amostra de postos
revendedores. Cada linha é o preço de um produto, em um posto, em uma data de coleta.

- Fonte: [Série Histórica de Preços de Combustíveis e de GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis),
  arquivos "últimas 4 semanas"
- Código: `pipelines/datasets/br_anp_precos_combustiveis/` e
  `models/br_anp_precos_combustiveis/`

## A fonte

A ANP publica três arquivos CSV em endereços fixos, substituídos a cada semana:

| arquivo | produtos |
|---|---|
| `ultimas-4-semanas-glp.csv` | GLP (botijão de 13 kg) |
| `ultimas-4-semanas-gasolina-etanol.csv` | gasolina, gasolina aditivada e etanol |
| `ultimas-4-semanas-diesel-gnv.csv` | diesel, diesel S10 e GNV |

Os arquivos são separados por `;`, em UTF-8 com BOM, com decimal em vírgula e datas no formato
`dd/mm/aaaa`. Cada um traz **só as últimas 4 semanas** de coleta: a cada semana entra
a mais recente e sai a mais antiga. A tabela acumula as semanas desde 2004; os
arquivos, não.

A mesma página publica a série histórica em arquivos semestrais (por exemplo,
`ca-2026-01.zip`). O flow não lê esses arquivos; eles servem para recuperar semanas que
já saíram dos arquivos de 4 semanas (ver [Recuperar semanas perdidas](#recuperar-semanas-perdidas)).

### Particularidades da fonte

- **A pesquisa é amostral.** Não cobre todos os postos nem todos os municípios: nas
  4 semanas até 02/10/2026, os arquivos têm 9.173 postos em 415 municípios. Um
  município fora da amostra não aparece na tabela.
- **As coletas se concentram de segunda a quinta-feira.** Sexta-feira é rara, e sábado
  quase não aparece.
- **A coluna "Valor de Compra" vem vazia** nos arquivos atuais, então `preco_compra`
  não é preenchido nas semanas recentes.
- **O arquivo não traz o código IBGE do município**, só o nome e a UF.
- **O formato servido já mudou.** Entre maio e junho de 2026, a ANP serviu planilhas
  XLSX nos endereços `.csv` e depois voltou ao CSV. O flow lê os dois formatos (ver
  `read_anp_file` abaixo).

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

O tratamento (`download_and_transform`, com as funções de `utils.py`) faz o que
depende do arquivo:

- **Leitura (`read_anp_file`)**: o formato é identificado pelos primeiros bytes do
  arquivo, não pela extensão. Um XLSX começa com `PK\x03\x04` (é um zip); qualquer
  outro conteúdo é lido como CSV. No XLSX que a ANP serviu em 2026, cada linha do CSV
  original foi cortada em cada vírgula, e cada pedaço ficou numa célula. A função junta
  de novo as células de cada linha com vírgula, mantendo as células vazias do meio, e
  lê o texto resultante como CSV separado por `;`. O resultado é o mesmo da leitura do
  CSV.
- **`id_municipio`**: o arquivo não traz o código, então o tratamento cruza o nome do
  município e a UF com `br_bd_diretorios_brasil.municipio`, com os nomes em maiúsculas
  e sem acento. Dois nomes do diretório são ajustados para coincidir com a grafia da
  ANP: `ESPIGAO D'OESTE` e `SANT'ANA DO LIVRAMENTO`. Os municípios que não casam ficam
  sem `id_municipio` e são listados no log como aviso.
- **`endereco_revenda`** junta rua, número e complemento, separados por vírgula.
- **`data_coleta`** passa para `aaaa-mm-dd`, e `ano` sai dela.
- Os preços trocam a vírgula decimal por ponto e o CEP perde o hífen. A região
  (`Regiao - Sigla`) é descartada.
- **`unidade_medida`** é padronizada pelo mapeamento `UNIDADES` de `constants.py`. Uma
  unidade fora do mapeamento interrompe o flow com `Unidade de medida desconhecida`,
  em vez de virar nula (como aconteceu na semana de 13 a 17/04/2026). Se a ANP mudar a
  grafia, inclua a nova em `UNIDADES`.

O modelo dbt converte os tipos, tira a pontuação do CNPJ e deixa os textos com a
primeira letra maiúscula (`initcap`). Por isso os produtos aparecem como `Gasolina`,
`Gasolina Aditivada`, `Etanol`, `Diesel`, `Diesel S10`, `Glp` e `Gnv`. As linhas que a
ANP repete no próprio arquivo (jun/2024, fev/2026) são mantidas como publicadas.

A tabela é **incremental** com `insert_overwrite`: cada `dbt run` regrava, a partir da
staging, os anos que caem na janela de 4 semanas do arquivo (da última data da tabela
menos 35 dias em diante). Assim, uma semana que a ANP republicar com correção entra
na tabela no run seguinte. Uma semana fora da janela, como a recuperada pelos arquivos
semestrais, só entra com `--full-refresh`.

Na staging, há uma pasta por data de coleta (`data_coleta=aaaa-mm-dd/data.csv`). Na
tabela final, a partição é por `ano`.

## Como o flow funciona

O pipeline segue a estrutura em estágios (`check_update → extract_and_load →
build_and_promote`), com dois flows do conjunto e um compartilhado:

1. **`check_update`** (`br_anp_precos_combustiveis_microdados_check_update`, agendado):
   - `get_data_source_anp_max_date` baixa o arquivo de GLP e pega a maior data de
     coleta. O arquivo de GLP representa os três, que trazem as mesmas datas.
   - `poll_source_for_update_task` compara essa data com o fim da cobertura da tabela
     em produção. Se a fonte estiver à frente, `commit_source_update_task` grava o
     `Update` da fonte e o `extract_and_load` é disparado.
   - Por último, `verificar_lacuna` confere se ficou alguma semana para trás (ver
     abaixo). Essa conferência roda depois do disparo, para que as semanas que ainda
     estão no arquivo sejam carregadas mesmo quando há lacuna.
2. **`extract_and_load`** (`br_anp_precos_combustiveis_microdados_download`):
   `download_and_transform` baixa e trata os três arquivos, e `make_partitions` grava um
   CSV por data de coleta. `check_rows_written` (`pipelines/utils/data_tests/rows.py`)
   compara as linhas gravadas com as da tabela tratada e interrompe a execução antes do
   upload se faltar ou sobrar linha. Os CSVs vão para a staging de dev.
3. **`build_and_promote`** (compartilhado, `pipelines/utils/metadata/flows.py`): roda e
   testa o dbt em dev; numa execução de produção, leva as pastas novas para a staging
   de produção, roda e testa o dbt lá e atualiza a cobertura.

A cobertura tem duas partes: as semanas mais recentes ficam restritas ao BD Pro, e o
restante é aberto. O `build_and_promote` recalcula esse corte a cada carga e recria as
regras de acesso no BigQuery, então a janela anda sozinha.

### Alerta de lacuna

O arquivo só traz 4 semanas. Se o flow ficar parado por mais tempo, as semanas mais
antigas saem do arquivo antes de serem carregadas. `verificar_lacuna` compara a
primeira data do arquivo com o fim da cobertura da tabela em produção e **falha o
`check_update`** quando a diferença passa de 7 dias. A falha se repete a cada execução
até a lacuna ser recuperada.

O limite de 7 dias vem da série: desde 2019, os únicos intervalos maiores que 7 dias
entre coletas são 2020 (ausente na própria ANP) e as lacunas de carga do pipeline.
Feriados não produzem intervalos desse tamanho.

**Execução verde não quer dizer que ingeriu.** Como a ANP publica uma vez por semana e
o flow roda todo dia, a maior parte das execuções termina com sucesso depois de
conferir a data no arquivo de GLP, sem carregar nada. Para saber se entrou dado,
procure no log `Há atualizações na fonte original` e um `extract_and_load` disparado,
ou veja se a cobertura andou.

## Agendamento

`0 10 * * *`, no horário de Brasília, no `check_update`: todo dia às 10h. A ANP
atualiza os arquivos uma vez por semana, sem dia fixo registrado; a execução diária faz
a semana nova entrar no primeiro dia depois da publicação.

## Testar no pool de dev

Dispare manualmente o deployment `dev-…check_update`. Uma execução de dev não leva nada
para produção (`promote_to_prod` é `False` fora dos deployments de produção), mas
grava o `Update` e o `Poll` da fonte no backend de produção quando encontra novidade.
Com a tabela em dia, o `check_update` termina sem disparar nada.

## Correções e reprocessamento

**Semana republicada pela ANP:** entra sozinha. Cada `dbt run` regrava os anos da
janela de 4 semanas a partir da staging, e a staging recebe os arquivos novos a cada
carga.

**Semanas atrasadas que ainda estão no arquivo:** entram na próxima execução com
novidade.

### Recuperar semanas perdidas

Uma semana que já saiu dos arquivos de 4 semanas só está nos arquivos semestrais da
série histórica (`dsas/ca/ca-AAAA-SS` para combustíveis automotivos e
`dsas/glp/glp-AAAA-SS` para GLP). Para recuperá-la:

1. gere as pastas `data_coleta=aaaa-mm-dd/data.csv` das datas que faltam a partir dos
   arquivos semestrais, com o mesmo tratamento do flow, e confira o resultado contra o
   arquivo da ANP antes de gravar;
2. grave as pastas na staging de dev e rode `dbt run --select
   br_anp_precos_combustiveis__microdados --full-refresh` e os testes em dev;
3. leve só essas pastas para a staging de produção com `transfer_files_to_prod_flow`
   (`folders` = as pastas novas) e reconstrua a tabela de produção com o deployment
   "BD template: Executa DBT model" (`target="prod"`, `flags="--full-refresh"`). O
   `table-approve` não serve aqui: ele substitui a staging de produção inteira pela de
   dev.

O `--full-refresh` é necessário porque a janela do `insert_overwrite` não alcança datas
antigas. Ele remove as regras de acesso do BD Pro, que são recriadas na próxima carga
do `build_and_promote`.
