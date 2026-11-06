# br_tse_filiacao_partidaria

Filiações partidárias registradas no Sistema de Filiação Partidária (Filia) do TSE.

| Tabela | Atualização |
|---|---|
| `microdados` | flow mensal deste diretório |
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

Enquanto o sistema estiver fechado, o `extract_and_load` falha. Como o `check_update` roda todo
dia, a carga do mês sai no primeiro dia em que o sistema voltar.

---

## Detecção de dado novo

Como a fonte não tem data, o `check_update` devolve uma data inventada: **o dia 1º do mês
corrente**. Ele roda todo dia à 1h23 e compara essa data com a "Última atualização na Base dos
Dados" (`Table.Update.latest`, com `compare_against="table_update"`), que guarda o dia em que a
tabela foi gravada pela última vez.

- A primeira execução do mês encontra a tabela com data do mês anterior e dispara a carga.
- As execuções seguintes do mesmo mês encontram a tabela em dia e terminam sem baixar nada.
- Se a carga falhar, a tabela continua com a data do mês anterior, e o dia seguinte tenta de novo.

No site, a "Última atualização na fonte original" (`RawDataSource.Update.latest`) mostra o dia 1º
do mês: é a data do flow, não uma data publicada pelo TSE.

Duas alternativas e o efeito de cada uma:

- **Ler a maior `data_filiacao` da fonte.** Exige baixar a lista inteira no `check_update` só
  para descobrir a data, e o `extract_and_load` baixaria tudo de novo em seguida.
- **Usar o dia 1º do mês seguinte.** A última atualização da tabela nunca alcança uma data no
  futuro, então o flow carregaria todo dia.

---

## Coleta

A unidade de coleta é o grupo: cada combinação de UF, município, zona e partido. A lista de
grupos sai dos três primeiros endereços, e cada grupo vira uma chamada à relação de filiados,
mesmo quando vazio.

- **Paginação.** O flow pede páginas de 10.000 até reunir o `totalElements` do grupo e falha se a
  contagem não bater. Na carga de agosto de 2026, o maior grupo tinha 7.090 filiados, e todo grupo
  cabia numa página só; as páginas seguintes só entram se algum grupo passar do limite.
- **Volume.** A coleta usa 16 threads, em blocos de 1.000 grupos. Cada bloco vai para o disco
  antes do seguinte, para não manter os cerca de 17 milhões de filiações na memória.
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
`data_extracao=`, e o `.sql` fica com a versão mais recente de cada `registro_filiacao`.

**O `append` mantém na tabela as filiações que saem da API.** Antes da primeira carga do flow, a
tabela de prod tinha 554.360 filiações que só existiam na extração de 2024-10-21 e 98.842 que só
existiam na de 2026-04-01: nenhuma voltou nas extrações seguintes. O motivo da saída é
desconhecido. Com `overwrite`, elas sumiriam na primeira carga.

**Custo.** A staging cresce cerca de 17 milhões de linhas por mês, e o `.sql` relê tudo a cada
execução.

## Município: cruzamento no `.sql`

O TSE identifica o município por um código próprio (`codLocalidadeTse`), e não pelo do IBGE. O
`.sql` faz o cruzamento com `basedosdados.br_bd_diretorios_brasil.municipio` pelo
`id_municipio_tse`, em `left join`, e ignora as colunas `id_municipio` e `id_municipio_tse` da
staging: nos arquivos antigos elas vêm preenchidas, nos novos vêm vazias.

O cruzamento fica no `.sql`, e não no `utils.py`. Com isso:

- o `utils.py` roda na máquina local, onde `bd.read_sql` falha, e a contagem de linhas pode ser
  conferida fora do Prefect;
- a limpeza não gasta a cota de consulta do projeto;
- o cruzamento acompanha o diretório: se ele ganhar um código que ainda não tem par, basta um
  `dbt run`, sem recarregar nada.

Em outubro de 2026, 26.963 linhas não tinham par no diretório.

---

## Faixa paga (BD Pro)

A tabela é `part_bdpro`, com a faixa paga definida pela `data_filiacao`: as filiações dos últimos
6 meses ficam no BD Pro. A configuração fica no backend, não no código: a coluna `data_filiacao`
marcada com `coverage_entity = date` e as duas Coverages, livre e paga.

- **Linhas sem `data_filiacao` ficam só no BD Pro.** A regra de acesso do público é
  `data_filiacao <= <fim da faixa livre>`, e uma data nula não passa nessa comparação. Na carga de
  agosto de 2026, eram 8.582 linhas.
- **Pela `data_extracao`, quase a tabela inteira cairia na faixa paga**, porque cada filiação fica
  só com a extração mais recente.

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

- Disparar o `dev-` do `check_update`. Ele faz o poll e, se a data da tabela for anterior ao dia
  1º do mês, dispara o `extract_and_load`, que baixa a lista inteira.
- O `build_and_promote` roda o dbt da `main`. Mudança no `.sql` ou no `schema.yml` se valida
  localmente, com `uv run dbt run --select br_tse_filiacao_partidaria__microdados`.
