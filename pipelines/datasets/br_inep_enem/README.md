# Documentação do conjunto: ENEM (Exame Nacional do Ensino Médio)

Microdados do ENEM publicados pelo INEP, uma edição por ano.

- Página da fonte: <https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem>
- Arquivo por edição: `https://download.inep.gov.br/microdados/microdados_enem_<ano>.zip`

## A mudança de estrutura de 2024

Até 2023 o INEP publicava um arquivo único por edição, que virou a tabela
`microdados`. A partir de 2024, por causa da LGPD, os microdados são distribuídos
em arquivos separados dentro do mesmo zip:

| arquivo no zip | tamanho (2024) | tabela |
|---|---|---|
| `DADOS/PARTICIPANTES_<ano>.csv` | 462 MB | `participantes` e `questionario_socioeconomico_<ano>` |
| `DADOS/RESULTADOS_<ano>.csv` | 1,68 GB | `resultados` |
| `DADOS/ITENS_PROVA_<ano>.csv` | 0,3 MB | não ingerido |

O corte separa quem prestou o exame de como se saiu nele, reduzindo o
cruzamento identificável. **`id_escola` vem mascarado** quando a escola teve menos
de dez participantes na edição.

O `id_escola` vem nulo da fonte em 64% das linhas. Dos preenchidos, 1,4% (2024) e
1,6% (2025) não estão em `br_bd_diretorios_brasil.escola`: são 7.830 códigos
distintos nas duas edições, dos quais cerca de 7.550 são os mascarados e 279 são
escolas que existem no Censo Escolar 2025 e ainda faltam no diretório (issue #2117).
O teste de relacionamento do `id_escola` desconsidera os nulos e aceita até 2% de
códigos fora do diretório.

`participantes` e `resultados` só existem de 2024 em diante. Pedir edição
anterior ao pipeline levanta erro em `resolve_years`, porque antes disso a fonte
não publica esses arquivos.

## Tabelas

### `participantes`

Uma linha por inscrito, particionada por `ano`. Sai de `PARTICIPANTES_<ano>.csv`,
descartando as colunas do questionário socioeconômico.

`indicador_treineiro` é `boolean` (verdadeiro/falso), igual à mesma coluna em
`microdados`. Na fonte ela vem como `0`/`1`, e a gravação converte para
`true`/`false` antes de subir para a staging (`constants.BOOLEAN_COLUMNS`). Sem a
conversão a coluna sairia nula, porque `safe_cast('1' as boolean)` devolve nulo.

### `resultados`

Uma linha por inscrito, particionada por `ano`. Sai de `RESULTADOS_<ano>.csv`.

**A edição de 2025 abriu a correção da redação por avaliador**, acrescentando 28
colunas: `nota_redacao_avaliador_1..4`, as vinte
`nota_redacao_competencia_<1..5>_avaliador_<1..4>` e
`presenca_redacao_avaliador_1..4`. É o que faz o arquivo da fonte crescer de
1,68 GB para 2,1 GB. As quatro `presenca_redacao_avaliador_*` são codificadas
como a `presenca_redacao` e estão no `dicionario`.

Quando uma edição não traz uma coluna que existe na tabela, essa coluna fica
**nula naquela edição**. Por exemplo, as 28 colunas de correção por avaliador são
nulas em 2024, porque só passaram a ser publicadas em 2025. O log do flow lista
quais colunas foram preenchidas com nulo em cada carga.

É o arquivo grande do conjunto. A limpeza lê em blocos e grava em fluxo, com um
`ParquetWriter` aberto por partição, de modo que a memória do pod não acompanha o
tamanho do arquivo. O flow pede `memory_limit` de 8Gi porque o `/tmp` do pod conta
contra a memória e o CSV extraído sozinho já ocupa 1,7 GB.

### `questionario_socioeconomico_<ano>`

**Uma tabela por edição**, sem partição, com `id_inscricao` e `q001` a `q023`. As
respostas vêm do mesmo `PARTICIPANTES_<ano>.csv`, nas colunas `Q001` a `Q023`.

Essa tabela **não tem flow agendado**, e é de propósito: cada edição é uma tabela
nova, que precisa de `.sql`, entrada no `schema.yml` e registro no backend antes
de existir. Um flow agendado falharia no poll de uma tabela que ainda não foi
criada. A transformação está em `utils.py` como as outras, e a carga de uma
edição nova é feita à mão depois que a tabela existe.

## `ano_conclusao`

`ano_conclusao` é o ano em que o participante concluiu o Ensino Médio, em `INT64`,
em `microdados` e em `participantes`. O INEP não publica esse ano do mesmo jeito em
todas as edições, e o `.sql` converte cada formato:

| edições | o que vem da fonte | conversão |
|---|---|---|
| 1998–2010 | a variável não existe; o ano de conclusão só aparece no questionário socioeconômico | nulo |
| 2011 | código de 1 a 8: `1` é 2010, `8` é 2003 | `ano - código` |
| 2012–2014 | o próprio ano | nenhuma |
| 2015 | código de 1 a 10: `1` é 2015, o ano do exame | `2016 - código` |
| 2016 em diante | código em que `1` é o ano anterior ao exame | `ano - código` |

O código `0` ("Não informado") e o valor vazio ficam nulos em todas as edições. Em
2013 a variável da fonte se chama `ANO_CONCLUIU`, sem o `TP_` das outras edições.

2015 é a única edição em que o código `1` é o próprio ano do exame. O dicionário
dela tem a mesma lista do de 2016, e a distribuição por idade mostra que a lista
está certa nas duas: entre os participantes de 18 anos que já concluíram, o código
mais frequente é o `2` em 2015 e o `1` em 2016, ambos o ano anterior ao exame.

### O menor ano acumula os anos anteriores

De 2015 em diante, o último código é "Antes de 2007" ("Anterior a 2007" em 2015 e
2016). Ele fica nulo, porque não corresponde a um ano.

De 2011 a 2014 o menor ano faz o mesmo papel, sem que o dicionário ou o Leia-me
digam isso: ele concentra quem concluiu naquele ano ou antes.

| edição | menor ano | participantes | ano seguinte | participantes |
|---|---|---|---|---|
| 2011 | 2003 (código 8) | 846.971 | 2004 | 156.172 |
| 2012 | 2003 | 766.447 | 2004 | 134.635 |
| 2013 | 2004 | 1.012.318 | 2005 | 178.720 |
| 2014 | 2004 | 1.152.253 | 2005 | 186.112 |

A idade mostra o acúmulo: em 2013, 65,3% dos participantes com 2004 têm 31 anos ou
mais, contra 15,3% dos que têm 2005.

Por isso o menor ano de 2011 a 2014 também fica nulo, pela mesma regra do "Antes
de 2007": 2003 em 2011 e 2012, 2004 em 2013 e 2014.

## O dicionário muda a cada edição

O `dicionario` é a parte do conjunto que uma edição nova mais costuma quebrar, e
não por descuido: há códigos do ENEM cujo significado depende do ano. Enquanto
faltarem, o `custom_dictionary_coverage` de `participantes` ou de `resultados`
falha, sempre com a mesma cara — `Got N results, configured to fail if != 0`.

**Os códigos de prova são renumerados todo ano.** `tipo_prova_ciencias_natureza`,
`_ciencias_humanas`, `_linguagens_codigos` e `_matematica` (os `CO_PROVA_*` da
fonte) recebem uma faixa nova a cada edição — 2025 trouxe 74 códigos inéditos.

**`situacao_conclusao` cita o ano no rótulo da fonte** — "concluirei o Ensino
Médio em 2025", "após 2025" (chaves 2 e 3). No `dicionario`, esses rótulos dizem
"no ano da edição do exame" e "após o ano da edição do exame", com uma linha por
chave, e uma edição nova só estende a cobertura. No `microdados` são duas linhas
por chave, uma para o texto de 1999–2010 ("Concluirá…") e outra para o de
2011–2023 ("Estou cursando e concluirei…").

### De onde saem os rótulos

Do `DICIONÁRIO/Dicionário_Microdados_Enem_<ano>.xlsx`, dentro do próprio zip, nas
abas `PARTICIPANTES_<ano>` e `RESULTADOS_<ano>`. São ~45 KB armazenados sem
compressão, então dá para lê-lo por requisição de faixa, sem baixar o zip.

**Não usar o `ITENS_PROVA_<ano>.csv` para os códigos de prova.** Ele só cobre a
aplicação regular — em 2025 vai até o código 1582, sem as reaplicações 1583–1634
— e o `TX_COR` traz a cor crua, com quatro `LARANJA` e três `ROXA` por área, sem
distinguir ledor, ampliada, superampliada ou libras.

### Como carregar

O dicionário é dado em bucket, não código:
`gs://basedosdados-dev/staging/br_inep_enem/dicionario/dicionario.csv`, arquivo
único com as colunas `id_tabela,nome_coluna,chave,cobertura_temporal,valor`.
**Sobrescrever esse mesmo nome** — subir com outro nome faria a tabela externa ler
os dois e duplicar o dicionário — e depois `dbt run --select
br_inep_enem__dicionario`.

Antes de estender a cobertura de uma linha de `2024` para `2024(1)2025`, conferir
que a chave continua no dicionário oficial da edição nova **e** que ela aparece no
dado. `cor_raca` mostra por quê: a chave `6` ("Não dispõe da informação") existe em
2024, o dicionário de 2025 lista apenas `0` a `5`, e nenhuma linha de 2025 traz o
`6`. Ela fica com cobertura `2024`, enquanto as outras seis passam a `2024(1)2025`.

## Como o pipeline está organizado

`constants.py` guarda tudo que varia por tabela, `utils.py` as funções puras,
`tasks.py` as `@task` que só as embrulham e `flows.py` a espinha `run_inep_enem`
mais um `@flow` por tabela.

Em `constants.py`, `RENAME` mapeia o nome da fonte para o da arquitetura e
`COLUMNS` fixa a ordem em que as colunas sobem para a staging, espelhando o
`.sql`. **A arquitetura manda**: quando ela e a fonte divergirem, o certo é o
dela, e as duas constantes se mexem junto com o `.sql`.

O trabalho acontece em `/tmp/br_inep_enem/<tabela>/`, com `input/` recebendo o
arquivo bruto e `output/` o particionado que sobe para o GCS. As edições são
baixadas e carregadas uma por vez, para o pod segurar um arquivo de cada vez, e o
`dbt` roda uma vez só no fim, porque materializa a tabela inteira a partir da
staging.

A gravação usa um schema fixo e todo texto. Sem ele, um bloco em que a coluna
venha inteira nula viraria tipo nulo e o `ParquetWriter` recusaria o bloco
seguinte por divergência de schema.

**O ano é conferido contra o conteúdo, não só contra o nome do arquivo.** A
edição a baixar sai do nome do zip listado na página do INEP, e o CSV é escolhido
pelo prefixo e pelo ano — mas a partição vem da coluna `ano` de dentro do
arquivo. `check_year` recusa o bloco cujo ano não seja o pedido, de modo que um
arquivo republicado com o nome de um ano e o dado de outro falhe alto em vez de
cair na partição errada. O questionário não tem coluna de ano e fica de fora da
conferência.

**Execução verde não quer dizer que ingeriu.** Quando o poll não acha novidade, o
flow encerra e o painel do Prefect fica verde do mesmo jeito — para saber se
entrou dado, leia o log ou veja se a cobertura andou.

## Backfill

Os dois flows aceitam `backfill_years`, uma lista de edições no formato `%Y`:

```json
{"backfill_years": ["2024"], "materialize_after_dump": false, "update_metadata": false}
```

Com `backfill_years` preenchida o flow pula o poll e o `Update` da fonte — uma
recarga de edição antiga não é novidade, e gravar o Update com ano anterior faria
a cobertura andar para trás. As edições são baixadas e carregadas uma por vez, e
o `dbt` roda uma vez só no fim.

Sem `backfill_years`, o flow carrega apenas a edição corrente da fonte.

## Armadilhas da fonte

**O certificado de `download.inep.gov.br` não valida.** As requisições vão com
`verify=False` e o aviso do urllib3 é silenciado uma vez, no import. O INEP não
publica soma de verificação, então não há como trocar isso por uma conferência de
integridade.

**O servidor derruba conexão com frequência**, às vezes já no handshake, às vezes
no meio dos 530 MB. Duas defesas, em camadas:

- a sessão HTTP (`build_session`) repete o estabelecimento da conexão e o erro
  transitório. Fica nela, e não só no `@task`, para valer também para quem chama
  as funções na mão;
- o `download_zip` **retoma de onde parou**. O `download.inep.gov.br` aceita
  `Range` (`accept-ranges: bytes`), então uma queda no meio deixa o pedaço em
  disco e a tentativa seguinte pede só o que falta, anexando ao arquivo. É por
  isso que o `input/` não é apagado entre tentativas — só os CSVs da edição
  anterior é que saem, antes de extrair a nova.

Duas proteções em volta disso: se o servidor ignorar o `Range` e devolver o corpo
inteiro (`200` em vez de `206`), o download recomeça do zero em vez de anexar e
corromper o arquivo; e ao final o tamanho é conferido contra o que o servidor
anunciou, para um encerramento limpo mas curto não passar por completo.

As tasks mantêm `retries=3` por cima de tudo.

**O nome do arquivo dentro do zip muda de edição para edição.** O CSV é escolhido
pelo prefixo e pelo ano, nunca por caminho fixo — o INEP já acrescentou sufixo de
revisão (`_V2`) em outros conjuntos depois da publicação.

**O método de compressão também muda, e a biblioteca padrão não lê todos.** O
`RESULTADOS_2025.csv` vem em **deflate64** (método 9), que o `zipfile` não
descomprime: `ZipFile.open` levanta `NotImplementedError: That compression method
is not supported`. As retentativas do `@task` não ajudam, porque o zip baixa
inteiro e o erro acontece ao abrir o membro.

É só esse membro — todo o resto da edição de 2025 está *store*, e 2024 é deflate.
`extract_member` cobre o caso: quando o método não é deflate64, usa o
`ZipFile.open` de sempre; quando é, lê o stream comprimido direto do arquivo, a
partir do fim do cabeçalho local do membro, e infla com **`inflate64`**, que já
vem instalado como dependência do `py7zr`. Confere o CRC ao final, porque esse
caminho não passa pela verificação que o `ZipFile.open` faz sozinho.

O pacote `zipfile-deflate64` do PyPI resolveria o mesmo problema, mas está parado
desde 2023, com wheels até cp310, e a imagem roda Python 3.12.

## Ao abrir uma edição nova

1. Conferir se o layout bate com o da edição anterior. Os arquivos
   `INPUTS/INPUT_SAS_*.sas` do próprio zip listam as colunas e têm poucos KB — dá
   para lê-los por requisição de faixa, sem baixar o zip inteiro.
2. Ajustar `RENAME` e `COLUMNS` em `constants.py` se a fonte mexeu em coluna, e o
   `.sql` junto.
3. Criar `.sql`, entrada no `schema.yml` e tabela no backend para o
   `questionario_socioeconomico_<ano>`.
4. Atualizar o `dicionario` com os códigos da edição, a partir do xlsx do zip:
   os `CO_PROVA_*` inteiros e a cobertura das chaves que seguem valendo, inclusive
   as de `situacao_conclusao`. Ver "O dicionário muda a cada edição".
5. Conferir no dicionário da edição que o último código de `TP_ANO_CONCLUIU`
   continua "Antes de 2007". Se o corte mudar, o `2007` do `.sql` de
   `participantes` e a descrição da coluna mudam junto. Ver "`ano_conclusao`".
6. Estender o `range` do `partition_by` nos `.sql` de `participantes` e
   `resultados` se a edição passar do `end` declarado. O `end` é exclusivo.
