# br_tse_filiacao_partidaria

Filiações partidárias registradas no Sistema de Filiação Partidária (Filia) do TSE.

| Tabela | Atualização |
|---|---|
| `microdados` | flow semanal deste diretório |
| `microdados_antigos` | estática (1990–2021), fora do flow |

---

## A fonte

Por trás da consulta pública do Filia (`https://filia2-consulta.tse.jus.br`) há uma API REST sem
documentação, a mesma que a página chama. O flow usa quatro endereços dela, todos sob
`/filia-consulta/rest/v1`:

| Endereço | Devolve |
|---|---|
| `/partidos` | todos os partidos, inclusive os extintos |
| `/localidade/{1..28}/municipios` | os municípios de cada UF, com o código do TSE |
| `/zona/municipio/{codigo}/zonasEleitorais` | as zonas eleitorais do município |
| `/relacao-filiados?sgUe&cdMunicipio&cdZona&sqPartido&currentPage&pageSize` | os filiados de um partido numa zona |

A relação de filiados devolve `{"entity": null, "entitys": [...], "totalElements": N}`, com no
máximo 10.000 registros por página.

**A API devolve a lista do momento da consulta.** Não há data de publicação, versão nem
histórico: cada chamada é um retrato do dia.

**As datas vêm como lista JSON**, `[2023, 5, 10]`, e não como texto.

### A Akamai só deixa passar `curl_cffi`

A API fica atrás da Akamai, que devolve 403 "Access Denied" para `requests` e `curl`, com ou sem
cabeçalho de navegador. O `curl_cffi` com `impersonate="chrome"` passa, porque a Akamai bloqueia
pela assinatura TLS, e não pelo cabeçalho nem pelo IP. O mesmo acontece com `us_dol_oflc` e
`us_ssa_beneficiaries`.

### O TSE tira o sistema do ar nas eleições

Em ano de eleição, o TSE fecha a consulta com o aviso "este sistema está temporariamente
indisponível durante o período das eleições. O acesso será restabelecido após o encerramento dos
trabalhos eleitorais.", sem data de retorno. Em 2026, o sistema estava fechado em 1º/10, antes do
1º turno, e aberto de novo em 6/11.

Com o sistema fechado, o `extract_and_load` falha toda semana, e a coleta volta no primeiro sábado
depois da reabertura (ver "Coleta que falha" abaixo).

---

## Detecção de dado novo

A fonte não tem data, então o `check_update` não detecta atualização: só dispara a coleta. Ele
roda todo sábado, à 1h23, e devolve **o dia da coleta, no fuso de São Paulo**.

Essa data chega ao `extract_and_load` em `download_params["reference_date"]` e dá nome à partição
`data_extracao=`. Assim, a partição e a "Última atualização na fonte original"
(`RawDataSource.Update.latest`) ficam com a mesma data. No site, essa data é o dia em que a Base
dos Dados consultou a API, e não uma data publicada pelo TSE.

O `check_update` compara o dia da coleta com a "Última atualização na Base dos Dados"
(`Table.Update.latest`, com `compare_against="table_update"`), que guarda o dia em que a tabela
foi gravada pela última vez. No sábado, essa data ainda é da semana anterior, e a carga é disparada.
Depois de uma carga bem-sucedida, uma nova execução no mesmo dia termina sem baixar nada.

Duas alternativas ficaram de fora:

- **Por que não ler a maior `data_filiacao` da fonte?** Para descobrir a filiação mais recente, o
  `check_update` teria de baixar a lista inteira, e o `extract_and_load` baixaria tudo de novo em
  seguida: seriam duas coletas completas a cada run.
- **Por que não rodar todo dia, para que uma coleta que falhou se repita sozinha?** Como a data é o
  dia da coleta, todo dia seria mais novo que a tabela, e o flow baixaria a lista inteira todos os
  dias. Com a coleta semanal, uma semana que falha é coberta pela seguinte.

### Coleta que falha

Uma coleta que falha, porque o TSE está fora do ar, a coleta foi interrompida ou o sistema está
fechado para as eleições, não se repete no mesmo dia. A próxima é no sábado seguinte, e a tabela
fica uma semana sem retrato novo.

Se for preciso o retrato antes disso, a carga pode ser disparada à mão pelo `check_update` de prod.
Ela grava o dia em que roda tanto na partição quanto na "Última atualização na fonte original".

---

## Coleta

A unidade de coleta é o grupo: cada combinação de UF, município, zona e partido. A lista de
grupos sai dos três primeiros endereços, e cada grupo vira uma chamada à relação de filiados,
mesmo quando vazio.

- **Paginação.** O flow pede páginas de 10.000 até reunir o `totalElements` do grupo e falha se a
  contagem não bater. Na carga de agosto de 2026, o maior grupo tinha 7.090 filiados, e todo grupo
  cabia numa página só; as páginas seguintes só entram se algum grupo passar do limite.
- **Volume.** A coleta usa 16 threads e mantém no máximo 64 grupos em andamento. Cada grupo vai
  para o disco assim que termina, para não manter os cerca de 17 milhões de filiações na memória.
  O log mostra o progresso a cada 10% dos grupos.
- **Falhas.** Cada chamada tem até 5 tentativas, com espera crescente. Uma resposta sem JSON
  interrompe o flow na hora, com uma mensagem que lembra o fechamento nas eleições.

## Tratamento

Cada registro vira uma linha com as colunas na ordem da staging:

- os 28 campos da API, de `sqRegistroFiliacao` a `numCpf`. O campo `sgUe`, que a API também
  devolve, fica de fora: repete a UF da pasta `sigla_uf=`;
- `id_municipio` e `id_municipio_tse`, gravadas vazias (ver "Município" abaixo).

| Campo | Tratamento |
|---|---|
| datas (`dtFiliacao`, `dtDesfiliacao`, `tsCadastroDesfiliacao`, `dtCancelamento`, `dtExclusao`) | lidas pelos três primeiros números da lista, o que cobre data e data com hora; data inválida, anterior a 1980 ou futura vira nulo |
| `nmEleitor`, `desSituacaoEleitor` | `title()` |
| `tpSexo` | `2` vira "Masculino", `4` vira "Feminino", qualquer outro valor vira nulo |
| demais | texto, como a API manda |

---

## Staging em `append`

A staging é CSV separado por vírgula, com cabeçalho, em pastas Hive
`data_extracao=AAAA-MM-DD/sigla_uf=XX/data.csv`. Cada carga acrescenta uma pasta
`data_extracao=`, e a tabela guarda todas: cada `data_extracao` é o retrato da lista que a API
devolvia naquele dia, e cada linha é uma filiação num retrato. O mesmo `registro_filiacao` aparece
uma vez em cada coleta em que a API o devolveu.

**O `append` mantém os retratos anteriores.** Com `overwrite`, cada carga apagaria as coletas
anteriores. Algumas filiações só existem num retrato antigo: 554.360 só aparecem na extração de
2024-10-21 e 98.842 só na de 2026-04-01, e nenhuma voltou nas extrações seguintes. O motivo da
saída é desconhecido.

**Tamanho.** Cada coleta tem cerca de 17 milhões de linhas, e a staging e a tabela crescem esse
tanto por semana.

| `data_extracao` | Linhas |
|---|---|
| 2024-10-21 | 17.173.764 |
| 2026-04-01 | 16.881.690 |
| 2026-08-04 | 16.881.616 |
| 2026-11-06 | 16.847.207 |
| 2026-11-09 | 16.848.155 |
| 2026-11-12 | 16.845.808 |

As três coletas de novembro de 2026 vêm dos runs de teste do flow em dev.

## Modelo incremental

O modelo é incremental, com `insert_overwrite` por `data_extracao`: cada run lê da staging só as
coletas posteriores à mais recente da tabela e grava as partições delas. As coletas anteriores não
são relidas.

- **O filtro compara texto com texto.** A `data_extracao` da staging é texto, porque vem do nome da
  pasta. O filtro a compara com a maior data da tabela convertida para texto. Com isso, o BigQuery
  lê só as pastas `data_extracao=` novas.
- **Uma coleta que já está na tabela não é refeita.** Se a pasta de uma coleta mudar na staging, ou
  se o `.sql` mudar, só um `--full-refresh` refaz as partições que já existem, e depois dele é
  preciso reaplicar as regras de acesso. Uma coleta refeita em outro dia vira outra partição, e a
  anterior continua na tabela até sair da staging e a tabela passar por um `--full-refresh`.
- **Os testes rodam só na coleta mais recente** (`__most_recent__(data_extracao)`), a única que
  cada run grava.

## Município: cruzamento no `.sql`

O TSE identifica o município por um código próprio (`codLocalidadeTse`), e não pelo do IBGE. O
`.sql` faz o cruzamento com `basedosdados.br_bd_diretorios_brasil.municipio` pelo
`id_municipio_tse`, em `left join`, e ignora as colunas `id_municipio` e `id_municipio_tse` da
staging: nos arquivos antigos elas vêm preenchidas, nos novos vêm vazias.

O cruzamento fica no `.sql`, e não no `utils.py`. Com isso:

- o `utils.py` roda na máquina local, onde `bd.read_sql` falha, e a contagem de linhas pode ser
  conferida fora do Prefect;
- a limpeza não gasta a cota de consulta do projeto;
- o cruzamento acompanha o diretório: se ele ganhar um código que ainda não tem par, as coletas
  novas já saem com o município, e as que já estão na tabela mudam com um `dbt run --full-refresh`,
  sem recarregar nada da fonte.

Em outubro de 2026, 26.963 linhas não tinham par no diretório.

---

## Faixa paga (BD Pro)

A tabela é `part_bdpro`, com a faixa paga definida pela `data_extracao`: os retratos dos últimos 6
meses ficam no BD Pro, e os anteriores ficam livres. A configuração fica no backend, não no código:
a coluna `data_extracao` marcada com `coverage_entity = date` e as duas Coverages, livre e paga.

A cada carga em prod, o `build_and_promote` calcula o fim da faixa livre, que é a `data_extracao`
mais recente menos 6 meses, e regrava a regra de acesso do público:
`data_extracao <= <fim da faixa livre>`. Por exemplo, com as coletas da tabela acima e uma nova em
2026-11-21, a faixa livre termina em 2026-05-21: as coletas de 2024-10-21 e 2026-04-01 ficam
livres, e as outras cinco, no BD Pro.

---

## Problemas conhecidos

### `data_cadastro_desfiliacao` quase sempre nula

Na carga de agosto de 2026, 749.892 das 749.911 desfiliações não tinham a data. Num grupo
conferido (SP, município 8852, zona 2739, MDB), `tsCadastroDesfiliacao` veio vazio nos 274
registros, inclusive nos 19 com `dtDesfiliacao`: a fonte parece não preencher o campo, mas um
grupo só não confirma. O tratamento atual lê o campo com qualquer horário; o anterior só lia
quando o horário era exatamente 1h00.

---

## Testar em dev

- Disparar o `dev-` do `check_update`. Ele faz o poll e, se a data da tabela for anterior à do
  dia, dispara o `extract_and_load`, que baixa a lista inteira.
- O `build_and_promote` roda o dbt da `main`. Mudança no `.sql` ou no `schema.yml` se valida
  localmente, com `uv run dbt run --select br_tse_filiacao_partidaria__microdados`. Para refazer
  as coletas que já estão na tabela de dev, acrescentar `--full-refresh`.
