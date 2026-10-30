# br_senado_dados_abertos

Dados legislativos do Senado Federal: senadores, partidos, blocos, lideranças, comissões, Mesa,
votações, orientações de bancada, processos, relatorias e discursos. As 18 tabelas vêm da API de
Dados Abertos Legislativos do Senado.

| Tabela | Grupo | Coluna de data |
|---|---|---|
| `senador` | A (cadastro) | — |
| `partido` | A (cadastro) | — |
| `bloco` | A (cadastro) | — |
| `lideranca` | A (cadastro) | — |
| `comissao` | A (cadastro) | — |
| `mesa` | A (cadastro) | — |
| `senador_mandato` | A (cadastro) | — |
| `senador_filiacao` | A (cadastro) | — |
| `senador_comissao` | A (cadastro) | — |
| `senador_cargo` | A (cadastro) | — |
| `votacao` | B (série por ano) | `data_sessao` |
| `votacao_parlamentar` | B (série por ano) | `data_sessao` |
| `votacao_orientacao_bancada` | B (série por ano) | `data_votacao` |
| `processo` | B (série por ano) | `data_apresentacao` |
| `relatoria` | B (série por ano) | `data_designacao` |
| `votacao_comissao` | B (série por ano) | `data_reuniao` |
| `votacao_comissao_parlamentar` | B (série por ano) | `data_reuniao` |
| `discurso` | B (série por ano) | `data_sessao` |

A coluna de data é a que o `check_update` compara com a cobertura e a que define a faixa do BD Pro.

## Fonte

API de Dados Abertos Legislativos do Senado Federal, em https://legis.senado.leg.br/dadosabertos.
É pública e não pede autenticação. O formato padrão da resposta é XML; com o cabeçalho
`Accept: application/json`, a API responde JSON. As requisições mandam um User-Agent de navegador.
A documentação OpenAPI fica em https://legis.senado.leg.br/dadosabertos/v3/api-docs.

Os dados administrativos do Senado ficam em outro conjunto,
`br_senado_dados_abertos_administrativos` (código em
`pipelines/datasets/br_senado_dados_abertos_administrativos/`), que lê também a API administrativa,
em https://adm.senado.gov.br/adm-dadosabertos.

Endpoints por tabela, com caminhos relativos à base da API:

| Tabela | Endpoint |
|---|---|
| `senador` | `/senador/lista/legislatura/{legislatura}`, em XML; uma chamada por legislatura |
| `partido` | `/composicao/lista/partidos` |
| `bloco` | `/composicao/lista/blocos` |
| `lideranca` | `/composicao/lideranca` |
| `comissao` | `/comissao/lista/colegiados` |
| `mesa` | `/composicao/mesaSF` |
| `senador_mandato` | `/senador/{codigo}/mandatos`; uma chamada por senador |
| `senador_filiacao` | `/senador/{codigo}/filiacoes`; uma chamada por senador |
| `senador_comissao` | `/senador/{codigo}/comissoes`; uma chamada por senador |
| `senador_cargo` | `/senador/{codigo}/cargos`; uma chamada por senador |
| `votacao`, `votacao_parlamentar` | `/votacao?dataInicio=<ano>-01-01&dataFim=<ano>-12-31`; uma chamada por ano |
| `votacao_orientacao_bancada` | `/plenario/votacao/orientacaoBancada/{ano}0101/{ano}1231` |
| `processo` | `/processo?ano=<ano>` |
| `relatoria` | `/processo/relatoria?ano=<ano>` |
| `votacao_comissao`, `votacao_comissao_parlamentar` | `/votacaoComissao/comissao/{sigla}`; uma chamada por comissão |
| `discurso` | `/plenario/lista/discursos/{AAAAMMDD inicio}/{AAAAMMDD fim}`; uma chamada por mês |

## Particularidades da fonte

### Corpo vazio e novas tentativas

Sob chamadas rápidas, a API às vezes responde 200 com corpo vazio. Toda requisição (`get_json`,
`get_xml_records`) tenta de novo até 6 vezes, esperando 1,5 s × o número da tentativa entre uma e
outra.

Se o 200 vazio se repete em todas as tentativas, a resposta é tratada como lista vazia. Um status
diferente de 200 que se repete, ou um erro de rede, levanta erro: uma falha da API nunca chega ao
tratamento como lista vazia.

### Chamadas por entidade

`senador_mandato`, `senador_filiacao`, `senador_comissao` e `senador_cargo` fazem uma chamada por
senador; `votacao_comissao` e `votacao_comissao_parlamentar`, uma por comissão; `discurso`, uma
por mês. Essas chamadas rodam em paralelo, 8 de cada vez.

Quando o endpoint de uma entidade falha em todas as tentativas, a chamada devolve `None` e a
entidade fica de fora daquele run. O run, que percorre milhares de entidades, segue.

### Dois formatos de resposta

Os endpoints mais novos (`/votacao`, `/composicao/lideranca`, `/processo`, `/processo/relatoria`)
devolvem uma lista plana, com chaves em camelCase. Os mais antigos devolvem envelopes aninhados,
com chaves capitalizadas: a lista de partidos, por exemplo, fica em `ListaPartidos` → `Partidos` →
`Partido`.

`get_nested_value`, de `utils.py`, percorre o envelope por uma lista de chaves e devolve `None`
quando falta algum nível. Falta nível quando não há dado, porque nesse caso a API omite o
envelope.

### Listas de um elemento

Nos envelopes do Senado, uma lista com um só elemento vira um objeto solto, e uma lista vazia some
do envelope. Todo nó que pode ser lista é normalizado para lista antes da leitura.

### Lista de parlamentares por legislatura

`/senador/lista/legislatura/{leg}` é lido em XML, não em JSON. O endpoint responde sem o envelope
`ListaParlamentarLegislatura`: o XML é uma concatenação sem raiz de elementos `<Parlamentar>`. Como
um objeto JSON não repete a mesma chave, a versão JSON junta a lista inteira num único registro,
sem erro nenhum.

`get_xml_records` embrulha o corpo sem raiz num elemento antes de ler, e lê também a resposta com
envelope. A leitura não tenta o JSON antes do XML: o JSON não distingue a resposta sem envelope de
uma lista vazia, e cada legislatura vazia passaria por um ciclo inteiro de tentativas (cerca de
30 s).

Legislaturas anteriores à cobertura da API respondem 200 com 0 bytes. A 36 é um exemplo; a 40 já
responde cerca de 92 KB. Pela regra do corpo vazio, isso é lista vazia, não erro.

### Leitura do XML

O XML é lido com `xml.etree.ElementTree`, da biblioteca padrão do Python. O `defusedxml` não está
na imagem do worker. O `fromstring` expande as entidades declaradas no próprio documento, mas não
busca entidades nem DTDs externos. Contra a expansão em cadeia de entidades (*billion laughs*), a
proteção é o limite de amplificação do expat, presente desde a versão 2.4.1; o Python 3.12 da
imagem do worker (`prefecthq/prefect:3-python3.12`) já vem com expat posterior a ela.

## Formato dos dados

Todas as colunas saem como texto no parquet: a staging é toda STRING, e o modelo dbt faz o
`safe_cast` para o tipo final. Nulo sai como nulo, nunca como o texto `"nan"`. Datas saem como
`AAAA-MM-DD`; data-hora, como `AAAA-MM-DD HH:MM:SS`.

`write_table` grava as séries particionadas por `ano` (`<tabela>/ano=<n>/data.parquet`) e os
cadastros num único `<tabela>/data.parquet`. O arquivo se chama sempre `data.parquet`, para que
uma recarga substitua a partição em vez de somar um arquivo novo a ela.

## Arquitetura

A ordem das colunas de cada tabela está no `COLUMNS` da classe da tabela em `constants.py`. Os
modelos dbt e o `schema.yml`, com tipos, descrições e testes, ficam em
`models/br_senado_dados_abertos/` e são editados à mão.

## Atualização

Prefect 3, no modelo em estágios: `check_update` → `extract_and_load` → `build_and_promote`. Cada
uma das 18 tabelas tem seu par `check_update`/`extract_and_load` em `flows.py`, montado por
`make_pipeline`, de `tasks.py`. As funções de leitura da API e de tratamento ficam em `utils.py`;
a task `extract_table`, em `tasks.py`, escolhe a função de cada tabela.

### Tabelas que usam as mesmas chamadas

Nenhum modelo dbt do conjunto faz `ref()` a outro, então as tabelas não têm ordem entre si e não
são agrupadas num flow só, mesmo quando dependem das mesmas chamadas à API. Cada tabela refaz as
chamadas de que precisa:

| Tabela | O que refaz |
|---|---|
| `votacao_parlamentar` | a extração de `votacao`, descartando a parte de `votacao` |
| `votacao_comissao` | `comissao`, para obter as siglas |
| `votacao_comissao_parlamentar` | `comissao`, para obter as siglas, e a extração de `votacao_comissao` |
| `senador_mandato`, `senador_filiacao`, `senador_comissao`, `senador_cargo` | `senador`, para obter os códigos dos senadores |

### Cadastros (grupo A)

Os 10 cadastros não têm data de publicação para comparar. O `check_update` devolve a data do dia
em que roda e compara com o `Table.Update` (`compare_against="table_update"`), que guarda a data
da última materialização, sempre anterior. A carga é disparada em todo run, e a tabela inteira é
refeita.

Um cadastro sem linhas faz o `extract_and_load` falhar antes da carga. Isso acontece quando a
API responde 200 com corpo vazio em todas as tentativas (`get_json` devolve None) ou quando o
envelope muda de forma; sem a trava, a tabela subiria vazia e o `build_and_promote` a levaria a
prod.

A materialização usa `NonHistorical`: só o `Table.Update.latest` é atualizado, e a cobertura
temporal registrada no onboarding não muda.

### Séries por ano (grupo B)

O `check_update` extrai a janela de rotina — o ano corrente e o anterior, para pegar edições
tardias (`recent_window` com `REFRESH_PRIOR_YEARS = 1`) — e lê a maior data da coluna de data da
tabela. A comparação é com a cobertura (`compare_against="coverage"`). Quando a janela vem vazia,
a data de referência é 1900-01-01, lida como "sem novidade".

A carga sobe só a janela, com `dump_mode="append"`: substitui as partições `ano=` tocadas e deixa
o histórico mais antigo como está. `votacao_comissao` e `votacao_comissao_parlamentar` não têm
janela de anos (ver a seção dessas tabelas).

As 8 séries restringem ao BD Pro os 6 meses mais recentes (`PartBdpro`, no `COVERAGE` da classe
de cada tabela em `constants.py`). A cada run, o `build_and_promote` recalcula a faixa e reaplica as row access
policies do BigQuery.

### Agendamento

Os `check_update` rodam todo dia, escalonados de 8 em 8 minutos, das 08:15 às 10:31 (horário de
Brasília), na ordem da tabela do início: primeiro os cadastros, depois as séries. O escalonamento
evita que todas as chamadas à API do Senado comecem ao mesmo tempo.

### Carga completa

A carga do histórico completo fica fora do Prefect, em `run_onboarding.py`, neste diretório. O
script chama `clean_all`, de `utils.py`, grava o parquet em `tmp/br_senado_dados_abertos/output/`
e, com `--upload`, sobe cada tabela para a staging de `basedosdados-dev` com `upload_to_gcs`, em
modo `overwrite`:

```bash
uv run pipelines/datasets/br_senado_dados_abertos/run_onboarding.py --full --upload
```

Sem `--full`, a extração é uma amostra (séries só de 2024, 15 senadores e 6 comissões), e o
upload é recusado. `--skip-extract --tables <tabela> ...` sobe de novo o parquet já gravado, sem
extrair outra vez.

O primeiro ano consultado em cada série:

| Tabelas | Primeiro ano |
|---|---|
| `votacao`, `votacao_parlamentar`, `votacao_orientacao_bancada` | 1991 |
| `processo` | 1900 |
| `relatoria` | 2015 |
| `discurso` | 1997 |

Anos sem dado não devolvem nada.

## Tratamento por tabela

### senador

A BD não tem diretório de senadores. `senador` é a dimensão do próprio conjunto. Trazem
`id_senador` as tabelas `senador_mandato`, `senador_filiacao`, `senador_comissao`,
`senador_cargo`, `lideranca`, `mesa`, `relatoria`, `discurso`, `votacao_parlamentar` e
`votacao_comissao_parlamentar`; `votacao_comissao` traz `id_senador_presidente`.

`clean_senador` lê a lista de parlamentares de cada legislatura, da 30 até a atual. A atual é
calculada pela data do run: a 57 vai de 2023 a 2027, e cada legislatura dura quatro anos. Na
virada de legislatura, o cálculo pode apontar uma legislatura à frente; a consulta a mais devolve
lista vazia.

Se nenhuma legislatura devolve parlamentar, `clean_senador` levanta erro e a tabela não é gravada.

`sigla_uf` vem do mandato (`Mandatos/Mandato/UfParlamentar`). A identificação
(`IdentificacaoParlamentar/UfParlamentar`) só é usada quando o mandato não traz UF. A identificação
traz UF em 97 dos 1.567 registros; o mandato, em todos. Nas legislaturas 30, 40, 50, 56 e 57, os
151 registros que têm as duas trazem a mesma UF.

Um senador aparece em várias legislaturas, e a tabela fica com uma linha por senador: a mais
completa (com mais colunas preenchidas) e, no empate, a da legislatura mais recente. O resultado
não depende da ordem em que a API devolve os registros.

A linha é mantida inteira: todas as colunas vêm da mesma legislatura. Para quem representou duas
UFs, `sigla_uf` é a da linha escolhida. Na API, 9 dos 1.567 senadores têm mais de uma UF, e nos 9
a linha escolhida traz a UF mais recente. O histórico por mandato fica em `senador_mandato`.

### mesa

A tabela traz a composição atual da Mesa do Senado, lida de `/composicao/mesaSF`. O endpoint não
traz histórico, e cada run substitui a tabela inteira.

### votacao e votacao_parlamentar

As duas tabelas saem da mesma extração. `votacao_parlamentar` guarda o contexto político da época
do voto (partido e UF) e o voto. Nome e sexo do senador são atributos fixos e ficam só em
`senador`.

Em `votacao`, as colunas de contagem somam os votos dos parlamentares naquela votação, pela sigla
do voto:

| Sigla do voto | Coluna |
|---|---|
| `Sim` | `voto_sim` |
| `Não` ou `Nao` | `voto_nao` |
| `Abstenção` | `voto_abstencao` |
| qualquer outra | `voto_outro` |

### processo

`sigla` e `numero` saem da identificação do processo, no formato `<SIGLA> <NUMERO>/<ANO>`; sufixos
como ` - CCJ` são ignorados. A partição `ano` é o ano da identificação ou, quando a identificação
não segue esse formato, o ano consultado na API.

### relatoria

O parâmetro `ano` da API filtra pelo ano de apresentação do processo, e a partição `ano` usa esse
mesmo ano, como em `processo`. A data da designação fica em `data_designacao`. A API só devolve
relatorias a partir de 2015, aproximadamente.

### votacao_comissao e votacao_comissao_parlamentar

A consulta é por sigla de comissão, e as siglas vêm de `comissao`. Cada run consulta a lista
inteira, sem janela de anos. Votação sem data de reunião é descartada, porque não tem partição
`ano`.

### discurso

O endpoint aceita no máximo cerca de um mês por consulta; um intervalo maior devolve HTTP 400.
Cada ano vira 12 consultas, uma por mês.

Os meses futuros da janela também respondem 400. As consultas de `discurso` tentam só 3 vezes, em
vez de 6, para não passar pelo ciclo inteiro de tentativas em cada um desses meses.
