# br_ibge_ppm — Pesquisa Pecuária Municipal

A PPM é uma pesquisa anual do IBGE, feita em todos os municípios do país, sobre o rebanho
existente na data de referência e a produção de origem animal do ano. O IBGE divulga o ano de
referência em setembro do ano seguinte.

Quatro tabelas, todas municipais e anuais:

| Tabela | O que traz | Desde |
|---|---|---|
| `efetivo_rebanhos` | cabeças por tipo de rebanho | 1974 |
| `producao_origem_animal` | quantidade e valor por produto (leite, ovos, mel, lã, casulos) | 1974 |
| `producao_aquicultura` | quantidade e valor por produto de aquicultura | 2013 |
| `producao_pecuaria` | vacas ordenhadas e ovinos tosquiados | 1974 |

## De onde vem o dado

Da API v3 de agregados do IBGE, um agregado do SIDRA por tabela. Cada requisição pede um ano,
uma variável, uma categoria e os 5.570 municípios de uma vez (`localidades=N6[all]`).

| Tabela | agregado | variáveis | classificação | categorias |
|---|---|---|---|---|
| `efetivo_rebanhos` | 3939 | 105 | 79 | 10 rebanhos |
| `producao_origem_animal` | 74 | 106 (quantidade), 215 (valor) | 80 | 6 produtos |
| `producao_aquicultura` | 3940 | 4146 (quantidade), 215 (valor) | 654 | 24 produtos |
| `producao_pecuaria` | 95 (ovinos tosquiados), 94 (vacas ordenhadas) | 108, 107 | — | — |

Até onde a fonte publicou se lê em `/agregados/<id>/metadados`, no campo
`periodicidade.fim` — é ele que o flow compara com o intervalo que a tabela já cobre. Quando
a tabela junta dois agregados, vale o menor dos dois anos: a linha só existe quando os dois
lados existem.

## Decisões

**Os subtotais não entram.** As três classificações publicam a categoria `0` (Total), e a da
aquicultura publica também `79366` (Peixes), que soma as categorias de peixe seguintes. As
duas somam linhas já presentes e dobrariam a produção do município, então a lista de
categorias em `constants.py` é a da classificação menos esses subtotais.

**A junção entre variáveis é externa.** Quantidade e valor vêm de variáveis diferentes do
SIDRA, e um município pode aparecer numa e faltar na outra. O código manual anterior usava
junção interna na `producao_pecuaria` e perdia município: em 2024 são 5.569 municípios contra
os 5.541 que estão na staging hoje.

**A sigla da UF sai do nome da localidade por expressão regular.** A API devolve hoje
`"São Paulo (SP)"` e antes devolvia `"São Paulo - SP"`; o código manual cortava no hífen, o
que hoje devolveria o nome inteiro no lugar da sigla. A expressão aceita os dois formatos.

**`-`, `..`, `...` e `X` são nulo.** São os símbolos que a API usa para dado inexistente,
valor arredondado a zero e dado omitido. Sem isso a coluna sobe como texto e o `safe_cast` do
`.sql` devolve nulo em silêncio.

**A `unidade` da `producao_origem_animal` só sai da variável de quantidade.** A unidade
descreve o produto (`Mil litros` para leite, `Mil dúzias` para ovos), e a variável 215 traz a
moeda do ano — `Mil Cruzeiros` até 1985, `Mil Reais` de 1994 em diante. Misturar as duas
deixaria a coluna alternando entre unidade de produto e nome de moeda.

**A staging sobe toda como texto.** É a convenção da casa, e o `.sql` faz `safe_cast` de cada
coluna. `astype(str)` não serve: escreveria nulo como a string `"nan"`, que o `safe_cast` não
desfaz.

**Os modelos descartam a linha vazia.** A fonte devolve uma linha para cada par município ×
produto, e cerca de 90% vem sem produção. O filtro está no `.sql`
(`where quantidade is not null`), não na limpeza — a staging guarda o que a fonte mandou.

## O que a carga manual deixou para trás

Até esta pipeline o conjunto se atualizava rodando script na mão, um par
`api_to_json.py` + `json_to_parquet.py` por tabela em `models/br_ibge_ppm/<tabela>/code/`.
Dois defeitos vieram de lá:

- **Faltavam dois rebanhos.** A lista de categorias do `efetivo_rebanhos` trazia
  `"267732796"`, que é `2677` (Ovino) e `32796` (Galináceos - total) grudados. A API aceita o
  id inventado e responde com a categoria em branco, então a tabela ficou com nove rebanhos,
  um deles sem nome, e **Ovino e Galináceos - total não existem em produção em nenhum ano**.
  Corrigido aqui: 2024 passa de 50.103 para 55.670 linhas na staging.
- **`producao_pecuaria` perdia município**, pela junção interna descrita acima.

Antes do primeiro run também é preciso **derrubar as quatro tabelas externas de staging**. O
script antigo gravava parquet tipado, e a definição das externas ficou com
`quantidade`/`valor` como `INT64`; lendo os arquivos novos, todos texto, o BigQuery recusa
(`has type BYTE_ARRAY which does not match the target cpp_type INT32`).

## Atualização

Um flow por tabela, todos chamando `run_ibge_ppm`. O horário fica na janela de divulgação —
dias 15 a 20 de setembro e de outubro —, porque um cron mensal olharia a fonte doze vezes por
ano para não fazer nada em dez.

Parâmetros que importam:

- `backfill_years` recarrega anos específicos (`["2023", "2024"]`) e, quando vem preenchido,
  pula o poll: é o caminho para recuperar ano que ficou para trás.
- `materialize_after_dump=False` e `update_metadata=False` mantêm a execução em dev. Os
  padrões escrevem em produção, mesmo quando o run sai do pool de teste.
- `force_run=True` ignora o poll.

O poll compara o ano publicado pela fonte com o intervalo de datas registrado na tabela. O de
produção diz 2024 desde a carga de 2024 que nunca chegou lá, então **a primeira execução
precisa de `backfill_years`**, ou ela encerra achando que não há novidade — e encerra verde.
