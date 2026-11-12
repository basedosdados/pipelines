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

Com o sistema fechado, o `extract_and_load` falha. Quando o fechamento inclui o dia 1º, a carga do
mês é disparada à mão quando o sistema volta (ver "Carga manual" abaixo).

---

## Detecção de dado novo

A fonte não tem data, então o `check_update` não detecta atualização: só dispara a coleta. Ele
roda no dia 1º de cada mês, à 1h23, e devolve **o dia da coleta, no fuso de São Paulo**.

Essa data chega ao `extract_and_load` em `download_params["reference_date"]` e dá nome à partição
`data_extracao=`. Assim, a partição e a "Última atualização na fonte original"
(`RawDataSource.Update.latest`) ficam com a mesma data. No site, essa data é o dia em que a Base
dos Dados consultou a API, e não uma data publicada pelo TSE.

O `check_update` compara o dia da coleta com a "Última atualização na Base dos Dados"
(`Table.Update.latest`, com `compare_against="table_update"`), que guarda o dia em que a tabela
foi gravada pela última vez. No dia 1º, essa data ainda é do mês anterior, e a carga é disparada.
Depois de uma carga bem-sucedida, uma nova execução no mesmo dia termina sem baixar nada.

Duas alternativas e o efeito de cada uma:

- **Ler a maior `data_filiacao` da fonte.** O `check_update` teria de baixar a lista inteira só
  para descobrir a data, e o `extract_and_load` baixaria tudo de novo em seguida.
- **Rodar todo dia, para que uma coleta que falhou se repita sozinha.** A data da tabela seria
  sempre anterior à do dia, e o flow baixaria a lista inteira todos os dias.

### Carga manual

O flow roda uma vez por mês, e uma coleta que falha no dia 1º não se repete sozinha. A carga é
disparada à mão, pelo `check_update` de prod, quando no dia 1º:

- o TSE está fora do ar ou a coleta é interrompida;
- o sistema está fechado para as eleições (ver acima). Nesse caso, a carga é disparada no
  primeiro dia em que o sistema volta.

A carga manual grava o dia em que roda tanto na partição quanto na "Última atualização na fonte
original".

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

- Disparar o `dev-` do `check_update`. Ele faz o poll e, se a data da tabela for anterior à do
  dia, dispara o `extract_and_load`, que baixa a lista inteira.
- O `build_and_promote` roda o dbt da `main`. Mudança no `.sql` ou no `schema.yml` se valida
  localmente, com `uv run dbt run --select br_tse_filiacao_partidaria__microdados`.
