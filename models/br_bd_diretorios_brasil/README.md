# Base de Dados — Diretórios Brasileiros

## 1. Visão geral

Conjunto de tabelas de diretório: entidades com chave primária (`municipio`,
`uf`, `escola`, `cbo_2002`, `cnae_2`, …) usadas como referência pelos demais
conjuntos da Base dos Dados. Diretório não tem cobertura temporal — é a tabela
que qualquer ano dos dados usa para resolver um `id_`.

- `gcp_dataset_id`: `br_bd_diretorios_brasil`
- slug no backend: `diretorios_brasil` (sem o prefixo `br_bd_`)
- 24 tabelas; só a `escola` tem flow Prefect

## 2. Como cada tabela é atualizada

| Tabela | Código no repo |
|---|---|
| `escola` | flow `br_bd_diretorios_brasil__escola` em `pipelines/datasets/br_bd_diretorios_brasil/` |
| `cid_10` | `code/cid_10.py` |
| `cnae_2` | `code/cnae_2.py` |
| `instituicao_ensino_superior` | `code/[update]instituicao_ensino_superior.ipynb` |
| demais | sem código no repo; carga manual |

## 3. escola

### Fonte

Catálogo de Escolas do Inep, servido por um portal OBIEE em
`https://anonymousdata.inep.gov.br/analytics/saw.dll`. A ação `Extract` aceita
os cookies anônimos que o portal entrega no primeiro GET, sem login. O download
é feito por `curl` em subprocesso porque o servidor derruba a conexão TLS
aberta pelo `ssl` do Python.

O `Extract` devolve no máximo 100.000 linhas e corta o resto sem avisar, na
ordem do código da UF. O Catálogo tem mais de 200 mil escolas, então o download
é feito uma UF por vez, com o filtro `P0=1`, `P1=eq`,
`P2="D - Localidade Escola"."Sigla Uf"`, `P3=<UF>`, e as partes são juntadas
num CSV só. A maior UF, SP, tem cerca de 33 mil escolas. Duas coisas a saber:

- o filtro precisa do nome interno da coluna, `Sigla Uf`. "UF" é só o rótulo
  exibido, e com ele o portal ignora o filtro e devolve o arquivo inteiro. Os
  nomes internos aparecem no cabeçalho da exportação com `Format=xml`, no
  atributo `saw-sql:displayFormula`;
- o download para com erro se alguma UF vier vazia ou com 100.000 linhas, que é
  o sinal de filtro ignorado ou de UF cortada.

No backend, a fonte original da tabela é "Catálogo de Escolas do Inep", e é a
única ligada a ela.

### O catálogo é o registro corrente, não o histórico

O catálogo traz as escolas que estão no registro do Inep hoje, e é o próprio
Inep que remove do registro as extintas em anos anteriores. Carregar o catálogo
puro, substituindo a tabela, apagaria do diretório as escolas removidas — cujo
`id_escola` continua aparecendo no censo escolar de anos anteriores.

Por isso a carga **une** três fontes e registra o resultado por escola na
coluna `situacao_catalogo`. Quando um `id_escola` aparece em mais de uma, vale
a primeira da lista:

1. o catálogo do dia, com todos os atributos;
2. o diretório já publicado (`fetch_diretorio_publicado`), para as escolas que
   saíram do catálogo, com os atributos da última vez em que apareceram;
3. o Censo Escolar (`fetch_censo_escolar`), para as escolas que nunca entraram
   no catálogo, com `id_municipio`, `sigla_uf` e os atributos que o Censo tem
   (ver abaixo), todos do último ano em que a escola aparece. O Censo não traz
   nome, endereço, telefone nem coordenadas.

Há uma exceção à ordem: uma escola que entrou pelo Censo passa a estar no
diretório publicado, e na carga seguinte seria copiada de lá, ficando para
sempre com os dados da primeira carga. Por isso as escolas `Ausente` sem nome
(o nome só vem do catálogo) são lidas de novo do Censo a cada carga. Só ficam
com a versão do diretório publicado se saírem do Censo.

| Valor | Significado |
|---|---|
| `Presente` | consta no catálogo mais recente |
| `Ausente` | não consta no catálogo; veio do diretório publicado (com atributos) ou só do Censo Escolar (nome, endereço, telefone e coordenadas vazios) |

O Censo Escolar é o que liga o diretório às tabelas históricas: sem ele, os
`id_escola` que aparecem no Censo e nunca entraram no catálogo ficariam sem
correspondência no diretório, no próprio Censo Escolar, no ENEM e no SAEB.

Consequência a conhecer: os atributos das escolas que saíram do catálogo vivem
só na tabela publicada. Rodar `clean_catalogo` sem passar `diretorio_publicado`
perde nome, endereço e coordenadas dessas escolas, porque o Censo não tem esses
campos. O aviso no log é a única proteção.

### Atributos vindos do Censo Escolar

As escolas que só existem no Censo recebem seis colunas, traduzidas de código
para o rótulo do catálogo por `constants.CENSO_TO_DIRECTORY`:

| Coluna no Censo | Coluna no diretório |
|---|---|
| `rede` | `dependencia_administrativa` |
| `tipo_localizacao` | `localizacao` |
| `tipo_localizacao_diferenciada` | `localidade_diferenciada` |
| `tipo_categoria_escola_privada` | `categoria_privada` |
| `conveniada_poder_publico` | `conveniada_poder_publico` |
| `tipo_regulamentacao` | `regulacao_conselho_educacao` |

A tradução não usa o dicionário de `br_inep_censo_escolar`. Nele, a localização
diferenciada tem as chaves 1 a 4, com 1 para "não diferenciada", mas o dado usa
0 para "não diferenciada", 1 a 3 para assentamento, terra indígena e quilombola,
e 8 para povos e comunidades tradicionais. A regulamentação nem está no
dicionário. Os códigos do mapa foram conferidos cruzando, escola por escola, o
Censo com o rótulo do catálogo. Alguns códigos chegam como `"0.0"`, e o sufixo
`.0` sai antes da tradução.

Muitas dessas colunas ficam nulas mesmo assim. Localização e dependência
administrativa vêm em todo ano do Censo, mas localização diferenciada e
regulamentação faltam na maioria das escolas que deixaram o Censo há anos, e a
`conveniada_poder_publico` está quase toda nula na tabela da BD.

### id_municipio

O catálogo traz o nome do município, não o código do IBGE, então `id_municipio`
é derivado de (nome, UF) contra o diretório `municipio`, em duas passagens:
`constants.MUNICIPIO_NAME_FIXES` para as divergências de grafia conhecidas (renomeações,
hífen, `z`/`s`), depois busca com o nome normalizado sem acento. Quando o nome
não resolve, vale o `id_municipio` que o Censo Escolar tem para a mesma escola.
Só fica nulo se a escola também não estiver no Censo, e a carga registra no log
os nomes não resolvidos e quantas escolas o Censo completou.

### Atualização

O flow `br_bd_diretorios_brasil__escola` roda uma vez por mês, no dia 5. Ele
baixa o catálogo (~85 MB), lê do BigQuery o diretório publicado, o diretório
`municipio` e o Censo Escolar, grava o parquet e materializa em dev e depois em
prod.

O catálogo não publica data de atualização, e a data do download não diz quando
o Inep atualizou o registro. Por isso o flow não consulta a fonte antes de
baixar: todo run baixa o catálogo e recarrega a tabela, e quem define a
frequência é o agendamento. A tabela é `NonHistorical`.

A limpeza falha se o catálogo vier com menos de 95% das escolas `Presente` do
diretório publicado (`constants.MIN_CATALOG_SHARE`). Um download incompleto não
apaga escola nenhuma, porque a união segura o diretório publicado, mas marcaria
como `Ausente` todas as escolas que faltaram, e o run terminaria verde.

O upload usa `dump_mode="append"`, não `"overwrite"`. O `overwrite` apaga a
tabela final antes de subir o arquivo, e em prod isso deixaria o diretório fora
do ar até o `dbt run` terminar. Como o parquet tem sempre o mesmo nome
(`escola/data.parquet`), o `append` substitui o arquivo da staging e a tabela
publicada continua de pé até o dbt trocá-la.

Depois de um run em dev, confira na tabela:

| Conferência | Valor |
|---|---|
| total | linhas já publicadas + escolas novas do catálogo + escolas que só aparecem no Censo Escolar |
| `id_escola` da tabela publicada fora da nova | 0 |
| `id_escola` de `br_inep_censo_escolar.escola` fora da nova | 0 |
| `id_escola` repetido | 0 |
| `id_municipio` nulo | 0 |
