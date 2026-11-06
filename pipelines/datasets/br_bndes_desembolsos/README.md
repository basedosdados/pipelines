# br_bndes_desembolsos

Desembolsos mensais do BNDES, publicados no Portal de Dados Abertos do BNDES (CKAN). Slug do conjunto no backend: `desembolsos`.

O backend de prod tem também um conjunto de slug `desembolso` (`br_bndes_desembolso`), sem tabelas e separado deste.

## Fonte

Pacote CKAN [`desembolsos-mensais`](https://dadosabertos.bndes.gov.br/dataset/desembolsos-mensais), licença ODbL. Descrição publicada pelo BNDES:

> Base de dados com desembolsos mensais com informações de porte do cliente, setor CNAE, Unidade da Federação, município, grupo de produtos, entre outros. Dados a partir de 1995.

O CKAN declara atualização a cada 3 meses. O pacote tem três recursos:

| Recurso | id | Destino |
|---|---|---|
| CSV "Desembolsos Mensais": ~754 MB, histórico inteiro desde 1995, republicado inteiro a cada versão | `179950b8-b504-4cc7-b0db-9c9eed99e9ba` | tabela `mensal` |
| PDF "Dicionário de dados de desembolsos mensais" | `d11d3295-abb5-438d-8a1d-847b0a135eff` | fica na fonte, acessível pelo link da fonte original |
| CSV "Mapeamento de BNDES para CNAE": correspondência entre as classificações do BNDES e a CNAE do IBGE | `94ca01fe-7554-49a2-9200-e9625a304b88` | fica na fonte, acessível pelo link da fonte original |

## Tabela `mensal`

Tem `ano` e `mes`; a partição é só por `ano`.

A tabela inteira é aberta (`AllFree`): a fonte atualiza a cada 3 meses, abaixo do limite mensal a partir do qual a BD reserva a janela mais recente a assinantes do BD Pro.

### Colunas

Contagens e ocorrências citadas abaixo se referem ao arquivo de 02/10/2026, cujo mês mais recente é 2026-03.

| Fonte | Tabela |
|---|---|
| `forma_de_apoio` | `forma_apoio` |
| `inovacao` | `indicador_inovacao` |
| `porte_de_empresa` | `porte_empresa` |
| `uf` | `sigla_uf` |
| `municipio_codigo` | `id_municipio` |
| `desembolsos_reais` | `valor_desembolsado` |

`ano`, `mes`, `produto`, `instrumento_financeiro`, `setor_cnae`, `subsetor_cnae_agrupado`, `setor_bndes` e `subsetor_bndes` mantêm o nome da fonte. `regiao` e `municipio` (o nome do município) ficam de fora; os dois saem de `sigla_uf` e `id_municipio` pelos diretórios.

- `uf` vem por extenso, em caixa alta e sem acento (`SAO PAULO`). Um de-para das 27 UFs em `constants.py` converte o nome na sigla; nome fora do de-para interrompe a limpeza.
- `municipio_codigo` tem o código `9999998`, com o município `DIVERSOS`. Nessas linhas `id_municipio` fica nulo e `sigla_uf` é mantida: a soma por UF inclui esses valores, a soma por município não. São 49.635 linhas (1,3%), com 21,6% do valor desembolsado, presentes em todos os anos de 1995 a 2026. Código fora de 7 dígitos também vira nulo; não há nenhum no arquivo.
- `inovacao` e `instrumento_financeiro` vêm vazios em todas as linhas de 1995 a 2001 (204.269 linhas), e só nelas. Ficam nulos.
- `desembolsos_reais` vem com vírgula decimal e sem ponto de milhar (`24753538073,6`). A vírgula vira ponto; valor fora desse formato interrompe a limpeza.

Os rótulos ficam como a fonte publica, em caixa alta. Nenhuma coluna tem o mesmo valor escrito de dois jeitos (diferença de acento, caixa, espaço ou pontuação). Em `instrumento_financeiro`, a caixa, o travessão e o acento dos nomes mudam de um programa para outro, e dois nomes têm espaço duplo, mantido.

## Pipeline

Flow único, toda segunda às 02:35 (America/Sao_Paulo).

### Detecção de novidade

A consulta à fonte é uma chamada só ao `resource_show` do CKAN. O flow lê o `last_modified` do recurso CSV e compara com a "Última atualização na Base dos Dados" (`Table.Update.latest`), via `poll_source_for_update_task(compare_against="table_update")`. Depois grava esse `last_modified` em "Última atualização na fonte original" (`RawDataSource.Update.latest`).

O `last_modified` muda quando o arquivo é substituído, não quando chega um mês novo. Uma correção sem mês novo também dispara a carga, que traz de novo o histórico inteiro, já corrigido.

No primeiro registro de metadados, `Table.Update.latest` precisa ficar um dia antes do `last_modified` do CSV; do contrário, o primeiro poll não enxerga novidade.

O flow não usa o mês do dado, lido pelo datastore do CKAN, como sinal: não se sabe se o datastore acompanha o CSV de 754 MB, e, se os valores estiverem gravados como texto, a ordenação por mês sai errada.

### Download e carga

1. Baixa o CSV do recurso e confere o MD5 contra o campo `hash` que o CKAN publica.
2. Lê o arquivo em blocos, trata as colunas como descrito em [Colunas](#colunas) e grava um parquet por ano, com todas as colunas em texto.
3. Sobe a staging com `dump_mode="overwrite"`, porque cada versão traz o histórico inteiro.

O conjunto não usa o pipeline em estágios (#1932): como cada carga traz o histórico inteiro, todas as partições mudam a cada execução, e promover só as partições alteradas equivale a promover todas.
