# Documentação do Conjunto de Dados: br_anatel_banda_larga_fixa (Acessos — Banda Larga Fixa)

Este documento centraliza o contexto da base de Acessos de Banda Larga Fixa da Anatel.
Serve de referência para futuros mantenedores.

---

## Sobre o Sistema

Os dados são publicados **mensalmente** pela Anatel e cobrem os acessos ao serviço de
Banda Larga Fixa (Serviço de Comunicação Multimídia — SCM). O microdado tem
granularidade fina: uma linha por empresa × tecnologia × velocidade × produto, por
município e mês. Além do microdado, o conjunto traz a densidade de acessos (por 100
domicílios) nos níveis Brasil, UF e município.

- Página do conjunto (dados.gov.br): <https://dados.gov.br/dados/conjuntos-dados/acessos---banda-larga-fixa>
- API de catálogo (o crawler usa p/ achar o recurso ZIP): <https://dados.gov.br/api/publico/conjuntos-dados/acessos---banda-larga-fixa>
- Arquivo ZIP dos dados (~1 GB): <https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip>
- Área técnica responsável na Anatel: PRUV (`pruv@anatel.gov.br`)

> O download vem do ZIP da Anatel, resolvido via API de catálogo em `get_zip_url`. O flow
> não baixa o ZIP inteiro: o servidor da Anatel aceita o cabeçalho HTTP `Range`, e
> `download_csv_from_zip` lê o ZIP remoto pela `HttpRangeFile` e extrai para
> `/tmp/data/input/` só o CSV da tabela: o `Acessos_Banda_Larga_Fixa_<ano>.csv` do ano mais
> recente no microdados e o `Densidade_Banda_Larga_Fixa.csv` nas densidades. As três
> funções ficam em `pipelines/shared/anatel/utils.py`, compartilhadas com a telefonia; o
> que muda entre os conjuntos (`API_URL`, `RESOURCE_FORMAT`, `MICRODADOS_FILE_PATTERN` e
> `DENSIDADE_FILE`) fica no `constants.py`. A landing page antiga
> (`gov.br/anatel/.../banda-larga-fixa`) está **404** e não é usada pela pipeline.

---

## Revisão histórica da fonte (microdados)

A Anatel **revisa meses passados para cima** conforme chegam registros atrasados.
Contagem de linhas por mês na fonte e em prod, medida em 2026-07-06:

| mês     | fonte  | prod   | diff          |
| ------- | ------ | ------ | ------------- |
| 2026-01 | 698274 | 696371 | +1903         |
| 2026-02 | 698074 | 692951 | +5123         |
| 2026-03 | 679799 | 671575 | +8224         |
| 2026-04 | 684785 | 669968 | +14817        |
| 2026-05 | 652876 | —      | novo na fonte |

Os diffs positivos crescem com a recência do mês (assinatura de revisão histórica). Cada
run do `microdados` regrava na staging todos os meses do ano mais recente, nos mesmos
caminhos, e a tabela é materializada como `table`. As revisões da fonte nos meses desse ano
entram na tabela junto com o próximo mês publicado.

> ⚠️ Isso vale para o **microdados**, que **não** regrediu. **NÃO vale para as
> densidades**: a fonte apagou 2023–2025 (ver seção Densidades), então ali o mesmo
> mecanismo causa **regressão**. `append` só é seguro quando a fonte não regride.

**Como re-verificar se o microdados atualizou** (a fonte muda todo mês — refaça os alvos
antes de comparar):

```sql
-- cobertura + contagem dos meses recentes no BigQuery
select ano, mes, count(*) as linhas
from `basedosdados.br_anatel_banda_larga_fixa.microdados`
where ano >= 2026
group by ano, mes
order by ano desc, mes desc;
```

Alvos da fonte: baixar o ZIP (link em "Sobre o Sistema"), ler os
`Acessos_Banda_Larga_Fixa_<ano>.csv` (`sep=';'`) e agrupar por `Ano`/`Mês`. Atualizou se
o mês-teto do BQ alcançou o da fonte e os meses recentes subiram para os valores dela.

---

## Tabelas e Particularidades

### br_anatel_banda_larga_fixa__microdados

Granularidade: uma linha por empresa × tecnologia × velocidade × produto por
município e mês. Sem chave única simples (por isso não há teste de unicidade).

**Tratamento.** `treatment` (`utils.py`) lê o CSV do ano com polars, todo como texto, e
grava um mês por vez em `ano=<ano>/mes=<mes>/sigla_uf=<uf>/data.csv`. Cada `data.csv` é
sobrescrito, nunca acrescentado. No CSV de 2026 (884 MB), o pico de memória fica entre 2,8 GB
(2 threads) e 3,7 GB (32 threads), e não cabe em 2 GB; por isso o `extract_and_load` do
microdados roda com `memory_limit` de 6Gi (`flows.py`).

**CNPJ com 14 dígitos.** O `.sql` completa o `cnpj` com zeros à esquerda
(`lpad(cnpj, 14, '0')`). Parte dos arquivos da staging foi gravada lendo o CSV como número,
o que tira o zero de cerca de 15% dos CNPJs. Com o `lpad`, todos os CNPJs de 2026 casam com
`br_bd_diretorios_brasil.empresa`.

**Range de partição.** A partição por `ano` estava com `end: 2023`, mas o dado já vai
até 2026 — os anos 2024–2026 (os mais consultados) caíam na partição de overflow
`__UNPARTITIONED__` e perdiam *partition pruning*. Corrigido para `end: 2031`
(convenção BD: último ano + 5). Como o model é `create or replace`, o novo
particionamento entra sozinho na próxima materialização completa.

**`acessos` conta acessos em serviço, não contratos.** A coluna traz o campo `Acessos` do
CSV, que a Anatel descreve como os acessos em serviço das prestadoras do SCM. O Glossário
das Telecomunicações ([Resolução Anatel nº 779/2025](https://informacoes.anatel.gov.br/legislacao/resolucoes/2025/2029-resolucao-779),
anexo, art. 1º) define acesso em serviço como o "acesso que está ativado e prestando
serviço ao usuário" (inciso II) e assinante como a "pessoa natural ou jurídica que firma
contrato com a Prestadora" (inciso XXXI). Um mesmo assinante pode ter vários acessos em
serviço ([Análise nº 304/2013-GCMB](https://www.anatel.gov.br/Portal/verificaDocumentos/documento.asp?numeroPublicacao=298286),
item 5.76).

**`acessos` NULL em 2017–2020.** A coluna `acessos` é nula em ~61–66% das linhas de
2017 a 2020 (e ~12% em 2007), com 0% nos demais anos. É **característica da fonte**, não
regressão: o padrão é idêntico em dev e prod e casa com um salto de volume (~3,3× de
2016 para 2017), sugerindo que a Anatel passou a emitir linhas decompostas sem o valor
de acesso preenchido. **Investigação pendente:** abrir os CSVs de 2017–2020 e comparar
os headers com `RENAME_MICRODADOS` (`constants.py`) para decidir entre documentar
(decomposição legítima) ou corrigir o parse.

### Densidades: `densidade_brasil`, `densidade_uf`, `densidade_municipio`

Densidade = acessos por 100 domicílios. Níveis geográficos consistentes em 1 (Brasil) /
27 (UF) / 5570 (município) por mês.

**A fonte apagou a densidade de 2023–2025.** O ZIP atual da Anatel publica 2023 e 2024
com as linhas em branco em todos os níveis (Brasil, UF e município) e quase todo 2025
vazio; retoma parcial em 2026. Prod ainda serve esses anos **completos**, materializado
de uma versão anterior da fonte. Por isso os `check_update` das 3 tabelas de densidade
estão **sem schedule** (sem `deploy_schedules` em `flows.py`): um run automático
re-materializaria do arquivo degradado e **regrediria o prod** (ex.: município 2025 cairia
de 66.840 → 250 linhas). Elas só rodam por disparo manual do `check_update`. Reativar o
cron somente após a decisão sobre a fonte (o apagão é
transitório — recálculo de densidade? — ou permanente? confirmar com a PRUV). O
`microdados` (acessos) **não** regrediu e mantém o schedule normal (`0 15 * * *`).

**Crons originais das densidades** (timezone `America/Sao_Paulo`), para restaurar ao
reativar o schedule:

| tabela | cron original |
| ------ | ------------- |
| `densidade_municipio` | `0 16 * * *` (16h) |
| `densidade_brasil` | `0 17 * * *` (17h) |
| `densidade_uf` | `0 18 * * *` (18h) |

**Por que só o `densidade_municipio` reprova o `unique_combination`.** O teste exige a
chave `[ano, mes, id_municipio]` única, e o que reprova é chave **repetida** (não valor
vazio). Nos anos apagados, Brasil e UF trazem **1 linha por mês** (a chave não repete →
passam), mas o município traz a **grade inteira** — 5.570 linhas/mês, todas com
`id_municipio` vazio → colapsam em `[ano, mes, NULL]` e reprovam. O tratamento resolve
com `dropna(subset=["Código IBGE"])` (remove as linhas sem código); a consequência é o
gap de 2023–2025.

**Como re-checar o apagão na fonte** (pra saber se a Anatel restaurou os anos): baixar o
ZIP, ler `Densidade_Banda_Larga_Fixa.csv` (`sep=';'`), filtrar `Nível Geográfico
Densidade = "Municipio"`. **Não basta contar linhas por `Ano`** — a soma pode chegar a
~66.840 com municípios **duplicados** ou **meses faltando**, mascarando um dado degradado e
levando a reativar em cima de fonte incompleta (regressão em prod). Antes de reativar, exija
completude **por mês**: para cada ano (2023/2024), confirme os **12 meses** presentes e
**~5.570 `Código IBGE` distintos em cada mês** (contagem de *distintos*, não de linhas). Só
com 12 meses × ~5.570 municípios é seguro reativar o schedule ou disparar o `check_update`
das densidades.
