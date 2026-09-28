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
`unit_column` marca a série de onde sai a `unidade`.

**Onde conferir na fonte.** O significado de `-`, `0`, `..`, `...` e `X` está em dois lugares:

- nas Notas técnicas da PPM, seção "Convenções". O IBGE as publica a cada ano desde 2017, em
  `https://biblioteca.ibge.gov.br/visualizacao/periodicos/84/ppm_<ano>_v<volume>_br_notas_tecnicas.pdf`;
  a de 2024 é a
  [ppm_2024_v52_br_notas_tecnicas.pdf](https://biblioteca.ibge.gov.br/visualizacao/periodicos/84/ppm_2024_v52_br_notas_tecnicas.pdf).
  O mesmo documento traz a definição de cada variável pesquisada e o questionário;
- no SIDRA, na página do agregado (`https://sidra.ibge.gov.br/tabela/<agregado>`, por exemplo
  [74](https://sidra.ibge.gov.br/tabela/74)): depois de gerar a tabela, no menu **Funções**, item
  **Símbolos especiais**. A redação é um pouco diferente da das Notas técnicas.

A unidade de uma variável num ano, como a moeda do valor da produção, que muda ao longo da
série, sai de duas APIs:

- a de agregados, que o flow usa, no campo `unidade`:
  `https://servicodados.ibge.gov.br/api/v3/agregados/74/periodos/1992/variaveis/215?localidades=N1[all]`;
- a do SIDRA, na coluna "Unidade de Medida", com vários anos de uma vez:
  `https://apisidra.ibge.gov.br/values/t/74/n1/all/v/215/p/1985,1986/c80/0`.

## Tratamento

**O total geral fica de fora, mas o `efetivo_rebanhos` tem categorias contidas em outras.** A
categoria `0` (Total), onde a classificação a publica, e a `79366` (Peixes) da aquicultura, que
soma as categorias de peixe seguintes, somam linhas já presentes e não entram na lista de
`constants.TABLES`. O `efetivo_rebanhos` traz as dez categorias da
classificação 79, e duas delas estão contidas em outras: `Galináceos - galinhas` em
`Galináceos - total`, e `Suíno - matrizes de suínos`, publicada desde 2013, em `Suíno - total`.
Somar a `quantidade` sem filtrar o `tipo_rebanho` conta esses animais duas vezes.

**A junção entre variáveis é externa.** Quantidade e valor vêm de variáveis diferentes, e um
município pode aparecer numa e faltar na outra; junção interna descartaria essas linhas, que os
modelos aceitam.

**A sigla da UF sai do nome da localidade por expressão regular**, que aceita os dois formatos
que a API usa: `São Paulo (SP)` e `São Paulo - SP`.

**`-`, `..`, `...` e `X` viram nulo.** Nas convenções das Notas técnicas, `-` é dado numérico
igual a zero não resultante de arredondamento, `..` é dado que não se aplica, `...` é dado não
disponível e `X` é dado omitido para não individualizar a informação. O `0` fica como número: é
zero resultante do arredondamento de um dado originalmente positivo. As fontes estão em "Onde
conferir na fonte". A troca usa dicionário, e não lista: `replace(lista, None)` faz o
pandas preencher para baixo em vez de anular.

**A `unidade` de `producao_origem_animal` sai da variável 106.** Ela descreve o produto
(`Mil litros` para leite, `Mil dúzias` para ovos), enquanto a variável 215 traz a moeda do ano
— `Mil Cruzeiros` até 1985, `Mil Reais` de 1994 em diante.

**Os modelos descartam a linha vazia.** A fonte devolve uma linha para cada par município ×
produto e cerca de 90% vem sem produção. O filtro está no `.sql`; a staging guarda o que a
fonte mandou. Nas tabelas com quantidade e valor (`producao_origem_animal` e
`producao_aquicultura`), sai a linha sem os dois
(`where quantidade is not null or valor is not null`): um município pode ter só o valor
preenchido, e são de 19 a 28 por ano na aquicultura.

**Em cinco anos da `producao_origem_animal`, o `0` quer dizer "sem produção".** Em 1974, 1975,
1992, 1993 e 1994, a fonte escreve `0` onde os outros anos trazem `-`. Na API, São Paulo
(`3550308`) aparece assim:

| Produto | 1991 | 1992 | 1995 |
|---|---|---|---|
| Lã | `-` | `0` | `-` |
| Mel de abelha | `-` | `0` | `-` |
| Casulos do bicho-da-seda | `-` | `0` | `-` |

São 47.188 linhas com `quantidade = 0` nesses cinco anos, nenhuma com valor positivo, e elas
dobrariam a tabela em 1992–1994. Nos demais anos, o `0` é o arredondamento da convenção: dos
7.107 zeros, 1.187 têm valor positivo, ou seja, houve produção. Por isso a regra vale só para os
cinco anos.

No valor, o `0` desses anos aparece também em linhas com produção: 11.002 em 1974, 11.026 em
1975, 10.842 em 1992, 5.712 em 1993 e 824 em 1994. Quase toda a produção de 1974 e 1975 vem com
valor 0.

Nos cinco anos, o modelo troca o `0` por nulo em quantidade e em valor, com um `case` em cada
coluna, e a linha sem os dois sai no filtro da linha vazia.

Para conferir se a fonte ainda publica assim:

```sql
select
    safe_cast(ano as int64) ano,
    countif(safe_cast(quantidade as int64) = 0) quantidade_zero,
    countif(safe_cast(quantidade as int64) = 0 and safe_cast(valor as int64) > 0)
        quantidade_zero_com_valor,
    countif(safe_cast(quantidade as int64) > 0 and safe_cast(valor as int64) = 0)
        valor_zero_com_producao
from `basedosdados-dev.br_ibge_ppm_staging.producao_origem_animal`
group by ano
order by ano
```

## Staging

O parquet sai **todo como texto**, como manda a convenção da casa, e o `.sql` faz o `safe_cast`
de cada coluna para o tipo da arquitetura. A API já entrega os números como texto (`"150"`), e o
`write_partitions` os grava como vieram. O nulo é gravado como `None`: `astype(str)` escreveria
a string `"nan"`, que o `safe_cast` não desfaz.

**A tabela externa guarda o schema de quando foi criada.** Em `dump_mode="append"`, o
`upload_to_gcs` só cria a tabela quando ela não existe, e o `_sync_staging_schema` acrescenta
coluna, mas nunca troca tipo. Uma tabela externa que declara `INT64` recusa o parquet de texto.
Para trocar o tipo, apague a tabela externa e os arquivos do prefixo no GCS e recarregue a série
inteira de uma vez.

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

Sem `backfill_years`, a carga termina no último ano publicado e começa no último ano registrado
ou no penúltimo publicado, o que vier antes. Sem intervalo registrado, começa no primeiro ano da
tabela. O intervalo é lido em produção, também nas execuções presas em dev.

**Por que um ano já carregado volta.** A PPM revisa o ano anterior a cada divulgação. Nas
[Notas técnicas de 2024](https://biblioteca.ibge.gov.br/visualizacao/periodicos/84/ppm_2024_v52_br_notas_tecnicas.pdf),
seção "Disseminação dos resultados", p. 7:

> Cabe ressaltar que, de acordo com a política de revisão de dados utilizada na pesquisa, ao
> divulgar os resultados de um ano, são revistos os do ano anterior.

O último ano registrado foi carregado na primeira versão, e a revisão dele só sai na divulgação
seguinte. Na divulgação de 2024, o flow recarrega 2023 junto. Se uma divulgação passa sem carga,
com a tabela registrada até 2021 e a fonte já em 2024, o flow carrega de 2021 a 2024: 2021 foi
revisado na divulgação de 2022, que ficou sem carga, e a cópia da tabela ainda é a primeira
versão.
