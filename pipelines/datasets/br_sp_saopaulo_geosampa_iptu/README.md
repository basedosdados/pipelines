# Documentação do Conjunto de Dados: br_sp_saopaulo_geosampa_iptu (IPTU de São Paulo)

Cadastro fiscal do Imposto Predial e Territorial Urbano (IPTU) do município de São
Paulo. Cada linha é um imóvel (número de contribuinte) em um ano de exercício.

- Fonte: [GeoSampa](https://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx),
  camada "IPTU" do grupo "Cadastro"
- Código: `pipelines/datasets/br_sp_saopaulo_geosampa_iptu/` e
  `models/br_sp_saopaulo_geosampa_iptu/`

## A fonte

O GeoSampa publica um arquivo ZIP por ano de exercício, desde 1995. Cada ZIP contém um
CSV separado por `;`, com cerca de 70 MB compactado e 1 GB descompactado. O endereço
de cada ano segue o modelo de `URL_TEMPLATE`, em `constants.py`.

### Particularidades da fonte

- **Um ano inexistente também responde HTTP 200.** No lugar do ZIP, o servidor devolve
  uma página HTML, então o status da resposta não indica se o ano existe.
- **O servidor exige headers de navegador**, definidos em `HEADERS`.
- **O servidor não informa o tamanho do arquivo** e ignora pedidos de download parcial.
- **O corpo da resposta vem compactado em gzip**, e o primeiro pedaço lido pode ter um
  único byte.
- **O formato do CSV muda entre os anos.** O arquivo de 1995 usa vírgula como separador
  decimal e não traz data de cadastramento; o de 2024 preenche números com zeros à
  esquerda (`0000136`); o de 2026 não. A coluna de esquinas aparece como
  `QUANTIDADE DE ESQUINAS FRENTES` ou `QUANTIDADE DE ESQUINAS/FRENTES`.
- **A codificação muda entre os anos.** Os arquivos de 1995 e 2026 estão em UTF-8; o
  de 2024, em latin-1.
- **Aspas não delimitam campos.** Toda linha tem exatamente 29 campos separados por
  `;`, mesmo quando um valor contém aspas. Há valores com aspas só de um lado
  (`"D N LT 12`), e um leitor de CSV padrão junta as linhas seguintes até encontrar
  outra aspa. Em 2024, isso faz a leitura padrão perder 75.065 linhas.
- **Uma linha de 1995 tem 20 campos vazios a mais.** A linha do contribuinte
  1691450011-7 tem os 29 campos e, depois deles, mais 20 campos vazios; o último
  campo real é só uma aspa (`"`). `normalizar_linhas` corta os campos vazios extras.

## O pipeline

### check_update

`ultimo_ano_publicado` (`utils.py`) testa os anos a partir do ano seguinte ao atual, em
ordem decrescente, e devolve o primeiro publicado. `ano_existe` lê só o início da
resposta e considera o ano publicado quando ela começa com `PK`, a assinatura de um
arquivo ZIP. Se nenhum dos `MAX_ANOS_BUSCA` anos testados existir, a função levanta um
erro, pois isso indica mudança no endereço da fonte.

O `check_update` devolve 1º de janeiro do ano encontrado, comparado contra o fim da
cobertura da tabela no backend (`date_format="%Y"`). Ele roda toda segunda-feira às
9h47 (horário de Brasília).

### extract_and_load

`iptu_extract_load_data` (`tasks.py`) recebe o ano encontrado pelo `check_update`,
baixa e trata esse ano e devolve a pasta `ano=<ano>/` em Parquet para o upload. A
cobertura não fica no código: o pipeline a lê do backend (`coverage_from_backend`), a
partir da coluna `ano` marcada com `coverage_entity="year"`.

`baixar_ano` (`utils.py`) grava o ZIP do ano em disco em pedaços de 1 MB, extrai o
único CSV que ele contém e apaga o ZIP. Se o arquivo baixado não for um ZIP, ou se
tiver mais de um CSV, a função levanta um erro. O download de 2026 levou cerca de 4
minutos e gerou um CSV de 938 MB, com 3.920.972 linhas.

`tratar_ano` lê o CSV em pedaços de 200 mil linhas, para não carregar o arquivo
inteiro na memória, e grava o resultado em `ano=<ano>/data.parquet`, com todas as
colunas como texto e valores vazios como nulos. Antes da leitura, `detectar_encoding`
percorre o arquivo e escolhe UTF-8 ou latin-1. A leitura usa `quoting=csv.QUOTE_NONE`,
para que as aspas sejam tratadas como texto. Ler o arquivo de 2026 de uma vez estoura
15 GB de RAM; em pedaços, o tratamento leva cerca de 1,5 minuto.

O tratamento para com erro, sem gravar nada, quando:

- alguma linha da fonte tem menos de `N_CAMPOS_FONTE` (29) campos, ou mais de 29 com
  conteúdo nos campos extras (`normalizar_linhas`);
- algum valor tratado foge do formato esperado da coluna (`validar_pedaco`):
  `numero_contribuinte` com 11 dígitos, `data_cadastramento` como `aaaa-mm-dd`,
  `COLUNAS_DECIMAIS` como `123` ou `123.45`, `COLUNAS_INTEIRAS` só com dígitos e as
  demais colunas sem espaços nas pontas. Um valor fora do formato indica coluna
  deslocada ou mudança na fonte, e viraria nulo em silêncio no `safe_cast` do dbt;
- o número de linhas gravadas difere do número de linhas da fonte, sinal de linhas
  perdidas ou juntadas.

Linhas com mais de 29 campos, mas cujos campos extras estão todos vazios (caso de
1995), são cortadas para 29 campos antes da leitura.

As checagens foram criadas a partir de defeitos encontrados no reprocessamento do
histórico, que passavam sem erro e só apareciam comparando valores:

- a leitura com aspas fez 2024 perder 75.065 linhas, sem aviso (checagem de número de
  linhas e de campos);
- valores entre aspas com espaços internos ficavam com espaços no fim depois que as
  aspas eram removidas (checagem de espaços nas pontas);
- `,89` virava `.89`, sem o zero antes do ponto, em 1 a 6 linhas por ano de 1995 a
  2015 (checagem de formato das colunas decimais).

As checagens de formato foram aplicadas aos 32 anos reprocessados, e todos passam.

`limpar_iptu` aplica as regras de padronização:

- renomeia as colunas conforme `RENAME`, o que também unifica as duas grafias da
  coluna de esquinas;
- remove as aspas que envolvem um valor inteiro (`"AP 321 BL ""C"""` →
  `AP 321 BL "C"`); aspas de um lado só são mantidas, e um valor que é só uma aspa
  vira nulo;
- remove espaços no início e no fim de todos os valores, antes e depois de tirar as
  aspas (`"APTO 51 BL.""A""  "` → `APTO 51 BL."A"`);
- troca a vírgula decimal por ponto e remove zeros à esquerda em `COLUNAS_DECIMAIS`,
  que inclui `valor_terreno` e `valor_construcao` (`0001465.00` → `1465.00`,
  `100,41` → `100.41`), e completa o zero antes do ponto (`,89` → `0.89`);
- remove casas decimais nulas (`2011.0` → `2011`) e zeros à esquerda
  (`0000136` → `136`) em `COLUNAS_INTEIRAS`, para que o `safe_cast` para `INT64` no
  dbt não resulte em nulo; `numero_notificacao` também passa por essa regra
  (`01` → `1`), como no staging anterior;
- remove o hífen de `cep` e `numero_contribuinte`;
- converte `data_cadastramento` de `dd/mm/aa` para `aaaa-mm-dd`, mantendo vazios os
  anos sem data.

Nos arquivos de 2024 e 2026, a saída tem o mesmo número de linhas da fonte (3.724.575
e 3.920.972), nenhum `numero_contribuinte` repetido e todos os valores numéricos e
datas convertem para o tipo final. A única coluna numérica com vazios é
`numero_imovel`, que já vem vazia na fonte.

## Correção do histórico

Os anos de 1995 a 2025 foram carregados manualmente pelo script
`models/br_sp_saopaulo_geosampa_iptu/code/iptu.py`, em duas cargas: 1995–2024 em julho
de 2024 e 2025 em dezembro de 2025. Todos os anos foram reprocessados com o código
deste pipeline e comparados com o staging anterior, contribuinte a contribuinte.

### Problemas do staging anterior

- **Colunas de valor corrompidas.** O script trocava a vírgula decimal por ponto e
  depois removia todo `.0` dos valores. Valores com centavos (`100.41`) não
  convertiam para `INT64` e viravam nulos; valores com centavos zerados (`2169.00`)
  ficavam dez vezes maiores (`21690`).

  | Anos | `valor_terreno` no staging anterior |
  |---|---|
  | 1995–2009 | 86,5% a 91,0% nulos; `valor_construcao` chega a 95,4% |
  | 2010–2021 | dez vezes maior que na fonte (mediana de 2021: 10.450, contra 1.045) |
  | 2022–2025 | iguais aos da fonte |

- **Linhas perdidas em 2024.** O script lia o CSV interpretando aspas, o que fez 2024
  perder 75.065 das 3.724.575 linhas (ver "Particularidades da fonte").
- **Linhas duplicadas em 2017.** O staging de 2017 tem 3.583.561 linhas para 3.383.561
  contribuintes: 200 mil linhas repetidas. O `select distinct` do modelo dbt remove as
  repetições, então a tabela final não foi afetada.

### Resultado do reprocessamento

- De 1995 a 2025, exceto 2024, o número de linhas e o conjunto de
  `numero_contribuinte` são idênticos aos do staging anterior. 2024 passa de 3.649.510
  para 3.724.575 linhas, e 2017 deixa de ter linhas repetidas no staging. 2026 é
  carregado pela primeira vez, com 3.920.972 linhas.
- `valor_terreno` e `valor_construcao` passam a ter os valores da fonte, com tipo
  `FLOAT64` no lugar de `INT64`, para preservar os centavos.
- Algumas centenas de linhas por ano passam a ter `NA` em `bairro`, como na fonte; o
  script anterior convertia esse texto em nulo.
- Linhas vazias na fonte passam a ter nulo, e não o texto `nan`.
- Espaços no fim de `complemento`, `bairro` e `referencia_imovel`, presentes no
  staging anterior de 2025, são removidos.
