# br_ibge_ppm — Pesquisa Pecuária Municipal

A PPM é uma pesquisa anual do IBGE, feita em todos os municípios do país, sobre o rebanho
existente na data de referência e a produção de origem animal do ano. O ano de referência é
divulgado em setembro do ano seguinte.

| Tabela | O que traz | Desde |
|---|---|---|
| `efetivo_rebanhos` | cabeças por tipo de rebanho | 1974 |
| `producao_origem_animal` | quantidade e valor por produto (leite, ovos, mel, lã, casulos) | 1974 |
| `producao_aquicultura` | quantidade e valor por produto de aquicultura | 2013 |
| `producao_pecuaria` | vacas ordenhadas e ovinos tosquiados | 1974 |

## A fonte

API v3 de agregados do IBGE, um ou mais agregados do SIDRA por tabela. Cada requisição pede
um ano, uma variável, uma categoria e os 5.570 municípios de uma vez (`localidades=N6[all]`).

| Tabela | agregado | variáveis | classificação | categorias |
|---|---|---|---|---|
| `efetivo_rebanhos` | 3939 | 105 | 79 | 10 rebanhos |
| `producao_origem_animal` | 74 | 106 (quantidade), 215 (valor) | 80 | 6 produtos |
| `producao_aquicultura` | 3940 | 4146 (quantidade), 215 (valor) | 654 | 24 produtos |
| `producao_pecuaria` | 95 (ovinos tosquiados), 94 (vacas ordenhadas) | 108, 107 | — | — |

Até onde a fonte publicou está em `/agregados/<id>/metadados`, no campo `periodicidade.fim`.
Numa tabela que junta dois agregados vale o menor dos dois anos: a linha só se monta quando os
dois lados existem.

`constants.TABLES` guarda esse mapa. Cada tabela lista as suas `series` — um par
agregado/variável, a coluna que a série alimenta e as categorias a pedir —, as colunas na
ordem publicada, a coluna de partição, a de rótulo da categoria e o primeiro ano da série.
`unit_column` marca a série de onde sai a `unidade`; `integer_columns`, as colunas que a
staging declara `INT64`.

## Tratamento

**Subtotais ficam de fora.** As três classificações publicam a categoria `0` (Total), e a da
aquicultura publica também `79366` (Peixes), que soma as categorias de peixe seguintes. Somam
linhas já presentes, então a lista em `constants.TABLES` é a da classificação menos esses dois.

**A junção entre variáveis é externa.** Quantidade e valor vêm de variáveis diferentes, e um
município pode aparecer numa e faltar na outra; junção interna descartaria essas linhas, que os
modelos aceitam.

**A sigla da UF sai do nome da localidade por expressão regular**, que aceita os dois formatos
que a API usa: `São Paulo (SP)` e `São Paulo - SP`.

**`-`, `..`, `...` e `X` viram nulo.** São os símbolos da API para dado inexistente, valor
arredondado a zero e dado omitido. A troca usa dicionário, e não lista: `replace(lista, None)`
faz o pandas preencher para baixo em vez de anular.

**A `unidade` de `producao_origem_animal` sai da variável 106.** Ela descreve o produto
(`Mil litros` para leite, `Mil dúzias` para ovos), enquanto a variável 215 traz a moeda do ano
— `Mil Cruzeiros` até 1985, `Mil Reais` de 1994 em diante.

**Os modelos descartam a linha vazia.** A fonte devolve uma linha para cada par município ×
produto e cerca de 90% vem sem produção. O filtro está no `.sql`
(`where quantidade is not null`); a staging guarda o que a fonte mandou.

## Staging

O parquet sai **tipado**, e não todo como texto: `constants.integer_columns` lista as colunas
que a tabela externa declara `INT64` e o `build_schema` monta o resto como texto. Os dois lados
têm que continuar batendo — o schema da externa fica congelado, porque em `dump_mode="append"`
o `upload_to_gcs` só cria a tabela quando ela não existe e o `_sync_staging_schema` acrescenta
coluna, nunca troca tipo. Texto numa coluna `INT64` faz o BigQuery recusar o arquivo.

Passar a staging para texto, como manda a convenção da casa, exige apagar as tabelas externas
**e** os prefixos no GCS e recarregar 1974–2024 de uma vez: os arquivos já gravados são todos
tipados.

Nas colunas de texto o nulo é gravado como `None` — `astype(str)` escreveria a string `"nan"`,
que o `safe_cast` do `.sql` não desfaz. Nas numéricas o tipo é o `Int64` do pandas, inteiro que
admite ausente, porque `astype(int)` não aceita nulo.

## Atualização

Um flow por tabela, todos chamando `run_ibge_ppm`, agendados todo dia às 14h de 15 de setembro a
31 de outubro. A PPM sai às 13h, numa data que muda a cada ano dentro dessa janela; o calendário
oficial está em `servicodados.ibge.gov.br/api/v3/calendario/9107`. Fora da divulgação, o poll
só consulta os metadados do SIDRA e encerra.

- `backfill_years` carrega anos específicos (`["2023", "2024"]`), pula o poll e não olha o
  intervalo de datas registrado.
- `materialize_after_dump=False` e `update_metadata=False` prendem a execução em dev. Os
  padrões dos dois são `True` e escrevem em produção, mesmo saindo do pool de teste.
- `force_run=True` ignora o poll.

O poll compara o ano publicado pela fonte com o intervalo de datas registrado na tabela em
produção. Quando o registro está à frente do que a tabela de fato tem, ele não vê novidade e o
flow encerra — em verde, sem carregar nada; carregar nesse caso pede `backfill_years`.

Sem `backfill_years`, o flow carrega todos os anos que faltam entre esse intervalo e a fonte, do
ano seguinte ao último registrado até o último publicado. Se uma divulgação passa sem carga, o
ano dela entra na execução seguinte, junto com o novo. Sem intervalo registrado, a carga começa
no primeiro ano da tabela; com o intervalo em dia, o que só passa do poll com `force_run`, ela
traz o último ano publicado. O intervalo é lido em produção, também nas execuções presas em dev.
