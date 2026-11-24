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
| CSV "Mapeamento de BNDES para CNAE": correspondência entre as classificações do BNDES e a CNAE do IBGE | `94ca01fe-7554-49a2-9200-e9625a304b88` | tabela `mapeamento_bndes_cnae` |

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

`setor_cnae`, `subsetor_cnae_agrupado`, `setor_bndes` e `subsetor_bndes` ficam com a inicial maiúscula e o resto minúsculo, como pede o manual de estilo da BD, e com os mesmos rótulos da tabela `mapeamento_bndes_cnae`, para que as duas se juntem pelo setor. Seis rótulos da fonte são escritos de outro jeito no arquivo do mapeamento, e um de-para em `constants.py` (`MENSAL_SETOR_LABELS`) usa a grafia do mapeamento:

| Coluna | Fonte | Tabela |
|---|---|---|
| `setor_cnae` | `AGROPECUÁRIA` | `Agropecuária e pesca` |
| `subsetor_cnae_agrupado` | `EQUIP INFO, ELETRONICO, ÓTICO` | `Equip info, eletrônico, ótico` |
| `subsetor_cnae_agrupado` | `MÁQ, APARELHO ELETRICO` | `Máq, aparelho elétrico` |
| `setor_bndes` | `INDUSTRIA` | `Indústria` |
| `subsetor_bndes` | `ATV. AUX. TRANSPORTES` | `Atividades auxiliares de transportes` |
| `subsetor_bndes` | `SERV. UTILIDADE PÚBLICA` | `Serviços de utilidade pública` |

Que cada par seja a mesma categoria é dedução pelo nome; nenhum documento do BNDES liga as duas grafias.

Os demais rótulos ficam como a fonte publica, em caixa alta. Nenhuma coluna tem o mesmo valor escrito de dois jeitos (diferença de acento, caixa, espaço ou pontuação). Em `instrumento_financeiro`, a caixa, o travessão e o acento dos nomes mudam de um programa para outro, e dois nomes têm espaço duplo, mantido.

## Tabela `mapeamento_bndes_cnae`

Diz em que setor e subsetor das classificações do BNDES cai cada código da CNAE 2.2 do IBGE. São as mesmas classificações das colunas `setor_cnae`, `subsetor_cnae_agrupado`, `setor_bndes` e `subsetor_bndes` da `mensal`. O mesmo arquivo (mesmo MD5) é publicado também nos pacotes `aprovacoes` e `consultas` do CKAN.

Não tem coluna de data nem partição.

### Colunas

Contagens e ocorrências citadas abaixo se referem ao arquivo com `last_modified` em 05/05/2025: 62 linhas na fonte, 109 na tabela.

| Fonte | Tabela |
|---|---|
| `setor_cnae_classificacao_bndes` | `setor_cnae` |
| `subsetor_cnae_agrupado_classificacao_bndes` | `subsetor_cnae_agrupado` |
| `codigo_cnae_ibge` | `cnae_2_secao`, `cnae_2_divisao`, `cnae_2_grupo`, `cnae_2_classe`, `cnae_2_subclasse` |
| `produto_bndes` | `regra_produto` |

`setor_bndes` e `subsetor_bndes` mantêm o nome da fonte. `produto_bndes` vira `regra_produto` porque não traz um produto, e sim os produtos a que a linha se aplica (`Todos`, `Exceto BNDES Exim`, `Somente BNDES Finame, BNDES Máquinas e Serviços, Cartão BNDES e BNDES Exim`…). Ela não se junta com a coluna `produto` da `mensal`.

- Os quatro rótulos de setor e a `regra_produto` ficam com a inicial maiúscula e o resto minúsculo, como pede o manual de estilo da BD, inclusive nas siglas: `Exceto BNDES Exim` vira `Exceto bndes exim`. Com isso, `Comércio e serviços` e `Comércio e Serviços`, que a fonte escreve dos dois jeitos em `setor_cnae_classificacao_bndes`, viram um valor só. A `mensal` usa os mesmos rótulos (ver o de-para na seção dela). Espaços repetidos e quebras de linha dentro das células viram um espaço.
- Dois subsetores CNAE agrupados vêm da fonte sem acento, e a limpeza acentua as palavras, como pede o manual: `Máq, aparelho eletrico` vira `Máq, aparelho elétrico`, e `Equip info, eletronico, ótico` vira `Equip info, eletrônico, ótico`. O de-para fica em `constants.py`.
- Uma célula de `codigo_cnae_ibge` pode ter vários códigos, e a tabela tem uma linha por código:
  - listas (`D352 e D353`, `K64, K65 e K66`) viram um código por item;
  - faixas de divisões (`A01 a A03`) viram uma divisão por número;
  - cada código vai para a coluna do seu nível, com os níveis acima preenchidos: `C10` só tem seção e divisão, `D351` vai até o grupo e `H4912401` até a subclasse. Os formatos são os do diretório `br_bd_diretorios_brasil.cnae_2`.
- A fonte publica a classe `H4911` sem o dígito verificador. A limpeza grava `4911`, e o modelo dbt completa a classe com o diretório `cnae_2`, pela classe que começa com esses 4 dígitos.
- `H49 (restante)` vira a divisão 49, com a regra `Exceto BNDES Exim` e o subsetor `Outros transportes`. Com a mesma regra, a divisão 49 tem linhas mais específicas: os grupos 492 e 493, a classe 4911 e as subclasses 4912401 e 4912402. A linha da divisão vale para o resto da divisão 49, o que a fonte indica com `(restante)`.
- O código `U99` com a regra `Todos` aparece em duas linhas da fonte, uma com o subsetor CNAE agrupado `Organismos internacionais` e outra com `Outros`; o resto é igual. As duas ficam na tabela, e por isso o teste de unicidade inclui `subsetor_cnae_agrupado`.

### Valores ausentes

`cnae_2_grupo`, `cnae_2_classe` e `cnae_2_subclasse` ficam nulas quando o código da fonte é de um nível acima: 101, 106 e 107 das 109 linhas. A linha vale para o nível inteiro, então o nulo não é dado faltando. As três estão no `ignore_values` do teste de nulos.

## Pipeline

Um flow por tabela, toda segunda (America/Sao_Paulo):

| Tabela | Horário |
|---|---|
| `mensal` | 02:35 |
| `mapeamento_bndes_cnae` | 02:40 |

### Detecção de novidade

A consulta à fonte é uma chamada só ao `resource_show` do CKAN. Cada flow lê o `last_modified` do recurso CSV da sua tabela e compara com a "Última atualização na Base dos Dados" (`Table.Update.latest`), via `poll_source_for_update_task(compare_against="table_update")`. Depois grava esse `last_modified` em "Última atualização na fonte original" (`RawDataSource.Update.latest`).

O `last_modified` muda quando o arquivo é substituído, não quando chega um mês novo. Uma correção sem mês novo também dispara a carga, que traz de novo o histórico inteiro, já corrigido.

No primeiro registro de metadados, `Table.Update.latest` precisa ficar um dia antes do `last_modified` do CSV; do contrário, o primeiro poll não enxerga novidade.

O flow não usa o mês do dado, lido pelo datastore do CKAN, como sinal: não se sabe se o datastore acompanha o CSV de 754 MB, e, se os valores estiverem gravados como texto, a ordenação por mês sai errada.

### Download e carga

1. Baixa o CSV do recurso e confere o MD5 contra o campo `hash` que o CKAN publica.
2. Trata as colunas como descrito nas seções de cada tabela e grava parquet com todas as colunas em texto: na `mensal`, lê o arquivo em blocos e grava um parquet por ano; no `mapeamento_bndes_cnae`, grava um parquet só.
3. Sobe a staging com `dump_mode="overwrite"`, porque cada versão traz o arquivo inteiro.

O conjunto não usa o pipeline em estágios (#1932): como cada carga traz o histórico inteiro, todas as partições mudam a cada execução, e promover só as partições alteradas equivale a promover todas.
