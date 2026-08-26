# Documentação do Conjunto de Dados: Indicadores do SIOPE (FNDE)

Indicadores legais e educacionais calculados pelo FNDE a partir das declarações
bimestrais de estados, Distrito Federal e municípios no SIOPE — Sistema de
Informações sobre Orçamentos Públicos em Educação. Cobrem a aplicação mínima em
MDE, os percentuais do FUNDEB, o investimento por aluno e o resultado do
exercício do ente.

---

## Sobre a fonte

Os dados são publicados na Plataforma Antonieta de Barros, em dois produtos:

| Produto | Conteúdo | Arquivo |
|---|---|---|
| 53 | 2021 a 2024 | `Indicadores_SIOPE_ate_2024.txt.gz` |
| 54 | exercício corrente | `Indicadores_SIOPE.txt.gz` |

Páginas: [produto 53](https://www.fnde.gov.br/plataforma-antonieta-de-barros/dados/produtos-de-dados/visualizar/53)
e [produto 54](https://www.fnde.gov.br/plataforma-antonieta-de-barros/dados/produtos-de-dados/visualizar/54).

O portal é uma SPA — a página não carrega link de download. O arquivo sai da API:

```text
GET https://www.fnde.gov.br/plataforma-antonieta-de-barros-api/products/data-products/{id}/artifact
```

O metadado do produto vem em `/products/data-products/{id}`.

Três restrições da API que condicionam o download e o poll:

- **`HEAD` responde 405** e a resposta não traz `Last-Modified`. Não há como
  checar atualização sem baixar.
- **`Range` é ignorado.** Download interrompido recomeça do zero.
- **O `updatedAt` do produto não acompanha o dado.** O campo marca a edição do
  registro do produto, não a exportação do arquivo; a data da exportação está no
  `DT_ATUALIZACAO` de dentro do arquivo. Um poll que leia o `updatedAt` perde
  atualização.

O produto 54 comprimido tem cerca de 2,6 MB e baixa em segundos; o 53 tem 45 MB
comprimidos e 1,4 GB expandidos. O 53 deve ser mantido comprimido e
descomprimido em stream na limpeza.

## 2025 não está publicado

O produto 54 se descreve como "Dados a partir de 2025", mas traz apenas o
exercício corrente: um download do 54 feito enquanto 2025 corria trouxe 2025 e
não 2026; hoje o mesmo endpoint traz 2026 e não 2025. O produto 53 vai de 2021 a
2024, e seu artifact responde 404 desde 2026-08-25.

**Nenhum dos dois produtos publica 2025 hoje.** 2025 entra na série pela cópia
daquele download, carregada à mão como o histórico. Não se sabe se, e por onde, a
fonte volta a publicá-lo — o flow lê apenas o produto do exercício corrente.

## Estrutura do arquivo

Texto separado por `;`, UTF-8, 14 colunas declaradas no cabeçalho:

```text
TIPO;NUM_ANO;NUM_PERI;COD_UF;SIG_UF;COD_MUNI;NOM_MUNI;COD_INDI;COD_EXIB;NOM_INDI;COD_GRUP;NOM_GRUP_INDI;VAL_INDI;DT_ATUALIZACAO
```

O cabeçalho é idêntico nos dois produtos, e as armadilhas abaixo valem para os
dois — uma função de limpeza serve para ambos.

### Linha estadual tem 12 campos, não 14

Em linha `TIPO=Estadual` os campos `COD_MUNI` e `NOM_MUNI` são **omitidos**, não
enviados vazios. O leitor tem que tratar o número de campos, não confiar no
cabeçalho: com 12 campos, o que vem depois de `SIG_UF` é `COD_INDI`.

```text
Estadual;2024;6;52;GO;42;1.2;Percentual de aplicação…;3;Indicadores…;70.69;2026-07-07 11:20:48.93005
Municipal;2024;6;52;GO;520870.000…;Goiânia;42;3.5;Percentual das despesas…;3;Indicadores…;…
```

### `COD_MUNI` tem 6 dígitos e vem como float

`520870.000000000000000000` para Goiânia, cujo código IBGE é `5208707`. Falta o
dígito verificador. Os 6 dígitos são exatamente o prefixo do código de 7, então
o `id_municipio` sai de join contra `br_bd_diretorios_brasil.municipio` — não de
cálculo do dígito.

### `COD_GRUP` e `NOM_GRUP_INDI` não carregam informação

`COD_GRUP` é `3` e `NOM_GRUP_INDI` é `Indicadores de Dispêndio com Pessoal` em
todas as linhas, inclusive nas de "Superávit/Déficit do ente federado" e "Valor
exigido de aplicação de impostos em MDE".

O agrupamento temático real está no prefixo do `COD_EXIB`: grupo 1 são as
aplicações mínimas legais, 2 despesas por etapa de ensino, 3 pessoal, 4
investimento por aluno, 5 IDEB, 6 receitas, 7 resultado do exercício, 8 valores
de MDE.

### `DT_ATUALIZACAO` é metadado do arquivo

Valor único por arquivo — a data em que o FNDE gerou a exportação, não a data do
registro.

## Grão e chave

Uma linha por ente, bimestre e indicador. `NUM_PERI` vai de 1 a 6 (bimestres).

A chave `TIPO + NUM_ANO + NUM_PERI + COD_UF + COD_MUNI + COD_INDI` é única em
toda a série, sem duplicata. O `dbt_utils.unique_combination_of_columns` de cada
modelo é o que verifica isso a cada carga.

Cobertura geográfica: 27 UFs e os municípios que declararam. O Distrito Federal
aparece somente como `Estadual`.

O exercício corrente é alvo móvel: o bimestre em coleta tem uma fração das linhas
de um bimestre fechado. Bimestre revisado reescreve linha já publicada.

## O catálogo de indicadores

São 68 códigos distintos e 95 pares esfera/indicador — 46 no Estadual, 49 no
Municipal.

### `COD_INDI` não identifica o indicador sozinho

27 códigos existem nas duas esferas, e **nos 27 o indicador é diferente**.
Nenhum código quer dizer a mesma coisa nas duas.

| `COD_INDI` | Estadual | Municipal |
|---|---|---|
| 42 | aplicação do FUNDEB na remuneração dos profissionais (1.2) | despesas com professores sobre a despesa com MDE (3.5) |
| 44 | receitas do FUNDEB não aplicadas (1.4) | investimento por aluno da educação infantil (4.1) |
| 65 | investimento por aluno da educação infantil (4.1) | IDEB séries iniciais (5.1) |
| 69 | investimento por aluno da EJA (4.5) | superávit/déficit do ente (7.1) |
| 94 | recursos do FUNDEB não utilizados (7.3) | valor aplicado em MDE (8.2) |

Dentro de uma esfera, `COD_INDI` e `COD_EXIB` são biunívocos em todos os anos —
nenhum código de exibição serve dois `COD_INDI` e vice-versa. A correspondência
não é monótona (`COD_INDI` 5 é o item 1.5; 40 é o 1.1).

### Os nomes mudam de ano para ano

Nove dos 95 pares esfera/indicador têm mais de um nome na série; os outros 86
mantêm o mesmo texto em todos os anos. As mudanças são de redação — citação da
Portaria Conjunta MGI/MF/CGU 33/2023, espaçamento, referência cruzada a outro
indicador — e nenhuma altera o que é medido.

**2022 é o ano divergente em todos os oito casos que mudam entre anos.** Em
quatro deles (Estadual 5, 40 e 42, e Municipal 24) o texto de 2022 é isolado: o
de 2021 volta idêntico em 2023 e segue até 2026. Nos outros quatro (Estadual 93,
Municipal 67, 89 e 90) há três textos — 2021, 2022, e 2023 em diante.

Um nome que vale, some e volta não é um intervalo contínuo, e a notação de
cobertura temporal da BD não expressa hiato.

### O Estadual 43 tem dois nomes dentro de 2024

Único caso de nome ambíguo no mesmo ano. O bimestre 1 de 2024 diz "máximo de
30%" e os bimestres 2 a 6 dizem "máximo de 40%", que é também o texto de 2021 a
2023 e de 2026. O limite legal não mudou nesse intervalo, então
o bimestre 1 registra um texto que a fonte corrigiu na exportação seguinte.

O `dicionario` da BD data seus valores por ano, então o bimestre isolado não é
representável: as duas linhas coexistem, ambas cobrindo 2024. A alternativa
seria descartar o texto do bimestre 1, que existe no dado publicado.

Por isso o nome vive no `dicionario`, cuja coluna `cobertura_temporal` data cada
valor, e não numa coluna da tabela de fato.

### O `dicionario` é fixo

As 112 linhas estão em `constants.DICTIONARY_ROWS` e são mantidas à mão, como em
`us_bls_qcew`, `us_fec_campaign_finance` e `br_sfb_sicar`. A tabela não é
particionada, então derivá-la de um produto só apagaria as linhas do outro.

A notação com ponta em branco significa "até onde a tabela vai", então `(1)` e
`2023(1)` seguem válidas quando um ano novo entra. A lista muda quando a fonte
reescreve um nome ou cria um indicador; nesse caso a limpeza registra um WARNING
com o par indicador/nome que não consta dela.

### O que 2026 acrescenta

Um indicador novo: o 1.9, "Percentual de aplicação FUNDEB vinculadas à Fomento
ETI (mínimo de 4%)", art. 212-A, XV da CF — `COD_INDI` 99 no Estadual, 95 no
Municipal. Nenhum indicador de 2024 desapareceu.

## Tabelas

Duas tabelas de fato, uma por esfera, mais o dicionário. Os modelos e os testes
estão em `models/br_fnde_fundeb/`.

**`indicador_estadual`**

```text
ano, bimestre, sigla_uf, id_indicador, codigo_indicador,
valor_percentual, valor_real
```

**`indicador_municipal`**

```text
ano, bimestre, sigla_uf, id_municipio, id_indicador, codigo_indicador,
valor_percentual, valor_real
```

Mapeamento das colunas:

| coluna | origem | derivação |
|---|---|---|
| `ano` | `NUM_ANO` | direto, partição |
| `bimestre` | `NUM_PERI` | direto, 1–6 |
| `sigla_uf` | `SIG_UF` | direto |
| `id_municipio` | `COD_MUNI` | 6 → 7 dígitos, join no diretório |
| `id_indicador` | `COD_INDI` | float → inteiro → string |
| `codigo_indicador` | `COD_EXIB` | direto |
| `valor_percentual` | `VAL_INDI` | quando o indicador é percentual |
| `valor_real` | `VAL_INDI` | quando o indicador é em reais |

Sete colunas da origem não sobem: `TIPO` (virou a tabela), `COD_UF` (redundante
com `sigla_uf`), `NOM_MUNI` (mora no diretório), `NOM_INDI` (vai para o
`dicionario`), `COD_GRUP` e `NOM_GRUP_INDI` (constantes), `DT_ATUALIZACAO`
(metadado do arquivo).

---

## Decisões

### 1. Duas tabelas, uma por esfera

O `COD_INDI` significa indicadores diferentes em cada esfera nos 27 códigos que
aparecem nas duas. Com uma tabela só, a chave do indicador passaria a ser o par
`(esfera, id_indicador)`, e o `dicionario` da BD não tem chave composta — o
schema é `id_tabela, nome_coluna, chave, cobertura_temporal, valor`, com `chave`
única. Separando por esfera, o `id_tabela` do dicionário resolve: o código 42
ganha uma linha em `indicador_estadual` e outra em `indicador_municipal`.

A tabela estadual também não carrega `id_municipio`, que seria nulo em todas as
suas linhas.

Consultas que abrangem as duas esferas exigem `union` das duas tabelas.

### 2. `valor_percentual` e `valor_real`, não uma coluna `valor`

A unidade varia por indicador — percentual em 57 dos 95 pares esfera/indicador e
reais em 36. Uma coluna `valor` única não teria `measurement_unit`, que a BD
exige em coluna numérica.

Fora as duas linhas do grupo 5, tratadas na decisão 3, cada linha preenche
exatamente uma das duas colunas — então nenhuma das duas chega perto de ser
integralmente não-nula. Por isso as duas entram em `ignore_values` no
`not_null_proportion_multiple_columns`, que exige 95% nas demais colunas.

### 3. Não existe coluna `valor_indice`

Os indicadores do grupo 5 (IDEB séries iniciais e finais) são a terceira
unidade, mas somam **duas linhas** no conjunto inteiro — Rio Branco, 2023,
bimestre 1, valor zero nas duas. Não há linha de IDEB em 2025 nem em 2026.

Uma coluna dedicada seria duas células não-nulas na série inteira. Essas duas
linhas sobem com `valor_percentual` e `valor_real` nulos, e o grupo 5 fica
registrado no `dicionario`.

### 4. O `id_municipio` sai de join, não de cálculo

O dígito verificador do código IBGE é calculável, mas a resolução contra
`br_bd_diretorios_brasil.municipio` acontece no dbt — padrão do repo — e o teste
`relationships` acende se algum código não casar.

O diretório tem a coluna `id_municipio_6`, então o join é por igualdade e não
por prefixo:

```sql
left join `basedosdados.br_bd_diretorios_brasil.municipio` as bd
    on t.id_municipio = bd.id_municipio_6
```

A staging carrega os 6 dígitos crus; o `id_municipio` de 7 dígitos nasce no
modelo. Todo código publicado casou com o diretório, e o teste `relationships`
acende se algum deixar de casar.

### 5. `id_indicador` e `codigo_indicador` convivem

São biunívocos dentro de cada tabela, logo redundantes. O `COD_INDI` é a chave
da fonte e o que o `dicionario` indexa; o `COD_EXIB` é o número usado para
identificar o indicador nos relatórios do FNDE.

---

## Carga e atualização

A série é carregada por dois caminhos — a máquina local e o worker —, com
partições disjuntas:

| | fonte | quem escreve |
|---|---|---|
| 2021–2024 | produto 53 | máquina local, chamando as tasks por `.fn()` |
| 2025 | cópia do produto 54 de quando 2025 era o exercício corrente | máquina local, idem |
| exercício corrente | produto 54 | worker, flow agendado |

O upload local escreve em `basedosdados-dev`; prod é materializado no merge pela
action `table-approve`. O prefixo de staging de dev **é** o histórico da série —
apagá-lo apaga o dado em prod no merge seguinte.

**Os dois caminhos compartilham o prefixo de staging, e a convivência depende de
`dump_mode="append"`.** O `if_exists="replace"` do upload age por blob, e o
`write_partitioned` grava sempre um `ano=<ano>/data.parquet` por partição: subir
o exercício corrente substitui a partição daquele ano e preserva as de 2021 a
2024. Bimestre revisado sobrescreve a partição em vez de duplicar linha.

`dump_mode="overwrite"` recria a tabela de staging e removeria o histórico.

O `dicionario` não entra no flow. As 112 linhas são fixas e mudam quando a fonte
reescreve o nome de um indicador, caso em que a limpeza registra um WARNING.

### Os módulos

| arquivo | o que faz |
|---|---|
| `constants.py` | identificadores, endpoints, mapas de índice e de unidade, e as 112 linhas do `dicionario` |
| `utils.py` | download e limpeza, sem Prefect: `download_product`, `clean_all` e o que elas usam |
| `tasks.py` | as `@task` que embrulham o `utils.py` |
| `flows.py` | o flow do exercício corrente, agendado nos dias 5, 12, 19 e 26 |

### Ordem do dbt na carga

`run` nas três tabelas **antes** de qualquer `test`. O
`custom_dictionary_coverage` das duas tabelas de fato lê o modelo do
`dicionario`, então testar uma tabela antes de o dicionário existir falha por
tabela ausente.

## Anomalias da fonte

Entram documentadas; não há como corrigir sem inventar dado.

### Percentual acima de 100

Vários indicadores percentuais passam de 100 legitimamente — aplicação acima do
mínimo exigido. Dois casos, os dois no Municipal, não são percentuais numa
fração isolada das suas linhas: o 2.8, despesas em educação sobre todas as
áreas, e o 6.3, receitas de transferências constitucionais, que nessas linhas
chegam à casa dos bilhões.

Nas demais linhas os dois se comportam como percentual. É valor em reais
vazando para um campo percentual.

### Percentual negativo

Parte das linhas municipais tem `valor_percentual` negativo, chegando a milhares
de pontos abaixo de zero. São quase todas do grupo 1, as aplicações mínimas
legais: o 1.3, aplicação do FUNDEB em MDE que não remuneração; o 1.1, aplicação
em MDE; e o 1.4, receitas do FUNDEB não aplicadas.

O valor vem negativo da fonte, não do parsing. A ocorrência cai a quase zero no
bimestre 6, o que aponta para declaração em aberto que é corrigida até o
fechamento do exercício.

### Indicadores integralmente zerados

Sete pares esfera/indicador têm zero em todas as linhas. Os dois maiores:

- Municipal 2.3, percentual do FUNDEB aplicado no ensino médio — município não
  mantém ensino médio;
- Municipal 4.13, investimento por aluno sobre o PIB per capita.

Os outros cinco: Estadual 4.13, Estadual 1.7, Estadual 1.6, e os dois do IDEB.
