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
cruzamento identificável. Uma consequência prática: **`id_escola` vem mascarado**
quando a escola teve menos de dez participantes na edição, então a coluna não
fecha com o diretório de escolas e está fora dos testes de relacionamento.

`participantes` e `resultados` só existem de 2024 em diante. Pedir edição
anterior ao pipeline levanta erro em `resolve_years`, porque antes disso a fonte
não publica esses arquivos.

## Tabelas

### `participantes`

Uma linha por inscrito, particionada por `ano`. Sai de `PARTICIPANTES_<ano>.csv`,
descartando as colunas do questionário socioeconômico.

`indicador_treineiro` é `0`/`1` na fonte e `BOOLEAN` na tabela publicada, como em
`microdados`. **A gravação converte os códigos para `'true'`/`'false'`**, em
`constants.BOOLEAN_COLUMNS`: a staging é toda texto e o BigQuery só aceita essas
duas palavras, então `safe_cast('1' as boolean)` devolve nulo. Foi o que
aconteceu com a carga de 2024 que está em dev, onde a coluna está inteiramente
nula — ela precisa ser refeita.

O manual de estilo pede `int64` para variável booleana preenchida com 0 ou 1, e
não lista `boolean` entre os tipos da BD. O conjunto segue `boolean` por
consistência com `microdados`, que é a mesma coluna e está em produção desde
2015.

### `resultados`

Uma linha por inscrito, particionada por `ano`. Sai de `RESULTADOS_<ano>.csv`.

**A edição de 2025 abriu a correção da redação por avaliador**, acrescentando 28
colunas: `nota_redacao_avaliador_1..4`, as vinte
`nota_redacao_competencia_<1..5>_avaliador_<1..4>` e
`presenca_redacao_avaliador_1..4`. Elas são nulas em 2024, que publicava só a
nota consolidada — é o que faz o arquivo da fonte crescer de 1,68 GB para
2,1 GB. As quatro `presenca_redacao_avaliador_*` são codificadas como a
`presenca_redacao`, mas **ainda não estão no `dicionario`**, e por isso ficaram
fora do `custom_dictionary_coverage`.

Coluna que uma edição não traz sai nula, e o log lista quais foram — a tabela
cobre de 2024 em diante e o INEP mexe no conjunto de colunas entre edições.

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

## O dicionário muda a cada edição

O `dicionario` é a parte do conjunto que uma edição nova mais costuma quebrar, e
não por descuido: há códigos do ENEM cujo significado depende do ano. Enquanto
faltarem, o `custom_dictionary_coverage` de `participantes` ou de `resultados`
falha, sempre com a mesma cara — `Got N results, configured to fail if != 0`.

Três casos, em ordem de quanto enganam:

**Os códigos de prova são renumerados todo ano.** `tipo_prova_ciencias_natureza`,
`_ciencias_humanas`, `_linguagens_codigos` e `_matematica` (os `CO_PROVA_*` da
fonte) recebem uma faixa nova a cada edição — 2025 trouxe 74 códigos inéditos.
Esse caso falha alto e é o menos perigoso.

**`ano_conclusao` é uma escala relativa à edição.** `1` é sempre o ano anterior ao
exame, e o último código é "Antes de <ano>". Entre 2024 e 2025 **todas** as chaves
mudam de significado:

| chave | em 2024 | em 2025 |
|---|---|---|
| 1 | 2023 | 2024 |
| 2 | 2022 | 2023 |
| 17 | 2007 | 2008 |
| 18 | Antes de 2007 | 2007 |
| 19 | — | Antes de 2007 |

Só a chave nova faz o teste falhar; as outras dezoito existem e passam, lendo
errado por um ano. **Uma edição nova exige o conjunto completo de
`ano_conclusao` com a cobertura temporal daquela edição**, não só o código novo.

**`situacao_conclusao` cita o ano no próprio rótulo** — "concluirei o Ensino
Médio em 2025", "após 2025" (chaves 2 e 3). As chaves não mudam, então o teste
nunca acusa; o texto é que fica defasado. As chaves 1 e 4 são estáveis.

O `microdados` já resolve `situacao_conclusao` assim: uma linha por edição para
as chaves 2 e 3, e cobertura composta (`1999(1)2001,2003(1)2010`) nas estáveis.

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
   os `CO_PROVA_*` inteiros, o conjunto completo de `ano_conclusao`, as chaves 2
   e 3 de `situacao_conclusao`, e a cobertura das chaves que seguem valendo. Ver
   "O dicionário muda a cada edição".
5. Estender o `range` do `partition_by` nos `.sql` de `participantes` e
   `resultados` se a edição passar do `end` declarado. O `end` é exclusivo.
