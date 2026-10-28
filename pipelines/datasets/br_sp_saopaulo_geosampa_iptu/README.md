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
