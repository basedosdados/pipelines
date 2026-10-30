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
cobertura da tabela no backend.

### extract_and_load

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
- algum `numero_contribuinte` tratado não tem 11 dígitos, sinal de coluna deslocada
  (`validar_pedaco`);
- o número de linhas gravadas difere do número de linhas da fonte, sinal de linhas
  perdidas ou juntadas.

Linhas com mais de 29 campos, mas cujos campos extras estão todos vazios (caso de
1995), são cortadas para 29 campos antes da leitura.

`limpar_iptu` aplica as regras de padronização:

- renomeia as colunas conforme `RENAME`, o que também unifica as duas grafias da
  coluna de esquinas;
- remove espaços no início e no fim de todos os valores;
- remove as aspas que envolvem um valor inteiro (`"AP 321 BL ""C"""` →
  `AP 321 BL "C"`); aspas de um lado só são mantidas, e um valor que é só uma aspa
  vira nulo;
- troca a vírgula decimal por ponto e remove zeros à esquerda em `COLUNAS_DECIMAIS`,
  que inclui `valor_terreno` e `valor_construcao` (`0001465.00` → `1465.00`,
  `100,41` → `100.41`);
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
de 2024 e 2025 em dezembro de 2025. O script trocava a vírgula decimal por ponto e
depois removia todo `.0` dos valores, o que corrompeu as colunas de valor:

- `2169,00` virou `21690` (dez vezes maior) e `39,08` virou `398`. Em prod, a mediana
  de `valor_terreno` fica entre 8 mil e 10 mil em 2015–2020, contra cerca de 1,5 mil a
  partir de 2022.
- Entre 1995 e 2005, de 86% a 95% de `valor_terreno` e `valor_construcao` são nulos,
  porque valores com centavos não convertem para `INT64`.

O script também lia o CSV interpretando aspas, o que fez 2024 perder 75.065 das
3.724.575 linhas (ver "Particularidades da fonte").

<!-- TODO: completar após o reprocessamento dos 31 anos -->
