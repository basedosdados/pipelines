# Documentação do Conjunto de Dados: br_anatel_banda_larga_fixa (Acessos — Banda Larga Fixa)

Este documento centraliza o contexto da base de Acessos de Banda Larga Fixa da Anatel,
consolidando a causa raiz do congelamento que travou a atualização das tabelas e as
decisões de engenharia adotadas no fix. Serve de referência para futuros mantenedores.

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

> O download real vem do ZIP da Anatel, resolvido via API de catálogo em
> `banda_larga_fixa/utils.py:download_zip_file`. A landing page antiga
> (`gov.br/anatel/.../banda-larga-fixa`) está **404** e não é usada pela pipeline.

---

## Causa raiz do congelamento (ponto crítico)

O flow desistia sozinho **antes de materializar**, por causa do poll eager
`register_source_poll_task` (`pipelines/crawler/anatel/banda_larga_fixa/flows.py`). Esse
poll comparava duas grandezas incompatíveis:

| lado da comparação                 | valor                 | o que é                                |
| ---------------------------------- | --------------------- | -------------------------------------- |
| `source_max` (calculado pelo flow) | `2026-04-01` (dia 01) | **cobertura** — último ano-mês do dado |
| "última atualização na fonte"      | `2026-06-04` (dia 04) | **data de publicação** do arquivo      |

A regra só atualiza se `source_max > api_latest`. Como a cobertura (abril, dia 01)
**sempre atrasa** em relação à data de publicação (4 de junho), o resultado era sempre
`False` → *"não há novas atualizações na fonte"* → o flow retornava sem materializar.
Ficaria travado até a cobertura passar de julho/2026.

O `2026-06-04` (dia 04) é um **resíduo envenenado**: este poll só grava datas de
cobertura com dia 01, então o dia 04 veio de um fluxo antigo / cadastro manual que
gravou a data de modificação do arquivo no campo que o poll lê. Além disso, o poll
eager tinha um segundo defeito: gravava o `Update` no momento da checagem, mesmo que o
flow morresse antes de materializar.

**Fix aplicado.** Migração para o poll **deferido**, espelhando
`pipelines/crawler/ibge_inflacao/flows.py`:

- `poll_source_for_update_task` apenas **detecta** a novidade antes de materializar;
- `commit_source_update_task` grava o `Update` **só depois** da materialização em prod.

Como o `commit` grava a `source_max` (cobertura, dia 01) como novo `api_latest`, e o
`upsert_raw_source_update` grava **sem trava monotônica**, esse commit **sobrescreve o
`2026-06-04` envenenado** por uma data de cobertura. A partir daí o poll compara
cobertura × cobertura (correto), e um crash no meio do flow não adianta mais o `Update`.

---

## Revisão histórica da fonte (microdados)

A Anatel **revisa meses passados para cima** conforme chegam registros atrasados.
Comparação da contagem por mês (fonte × prod, verificação 2026-07-06) — a fonte já tinha
maio/2026 enquanto o prod estava preso em abril:

| mês     | fonte  | BQ (prod, congelado) | diff          |
| ------- | ------ | -------------------- | ------------- |
| 2026-01 | 698274 | 696371               | +1903         |
| 2026-02 | 698074 | 692951               | +5123         |
| 2026-03 | 679799 | 671575               | +8224         |
| 2026-04 | 684785 | 669968               | +14817        |
| 2026-05 | 652876 | —                    | novo na fonte |

Os diffs positivos crescem com a recência do mês (assinatura de revisão histórica). As 4
tabelas são materializadas como `table` (**override**) e o upload é `append`, então o
`force_run` re-materializa do staging revisado e realinha sozinho — backfill idempotente
por partição.

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

**Range de partição.** A partição por `ano` estava com `end: 2023`, mas o dado já vai
até 2026 — os anos 2024–2026 (os mais consultados) caíam na partição de overflow
`__UNPARTITIONED__` e perdiam *partition pruning*. Corrigido para `end: 2031`
(convenção BD: último ano + 5). Como o model é `create or replace`, o novo
particionamento entra sozinho na próxima materialização completa.

**`acessos` NULL em 2017–2020.** A coluna `acessos` é nula em ~61–66% das linhas de
2017 a 2020 (e ~12% em 2007), com 0% nos demais anos. É **característica da fonte**, não
regressão: o padrão é idêntico em dev e prod e casa com um salto de volume (~3,3× de
2016 para 2017), sugerindo que a Anatel passou a emitir linhas decompostas sem o valor
de acesso preenchido. **Investigação pendente** (não bloqueia o fix do congelamento):
abrir os CSVs de 2017–2020 e comparar os headers com `RENAME_MICRODADOS`
(`constants.py`) para decidir entre documentar (decomposição legítima) ou corrigir o
parse.

### Densidades: `densidade_brasil`, `densidade_uf`, `densidade_municipio`

Densidade = acessos por 100 domicílios. Níveis geográficos consistentes em 1 (Brasil) /
27 (UF) / 5570 (município) por mês.

**A fonte apagou a densidade de 2023–2025.** O ZIP atual da Anatel publica 2023 e 2024
com as linhas em branco em todos os níveis (Brasil, UF e município) e quase todo 2025
vazio; retoma parcial em 2026. Prod ainda serve esses anos **completos**, materializado
de uma versão anterior da fonte. Por isso as 3 tabelas de densidade estão **sem schedule**
(`cron=None` em `flows.py`): um run automático re-materializaria do arquivo degradado e
**regrediria o prod** (ex.: município 2025 cairia de 66.840 → 250 linhas). Elas só rodam
via `force_run` manual. Reativar o cron somente após a decisão sobre a fonte (o apagão é
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
Densidade = "Municipio"`, agrupar por `Ano` e contar as linhas com `Código IBGE`
preenchido. Se 2023/2024 voltarem a ter ~66.840 códigos preenchidos, o apagão acabou →
dá pra reativar o schedule/`force_run` das densidades.

---

## Estrutura dos Flows

Um flow por tabela em `pipelines/datasets/br_anatel_banda_larga_fixa/flows.py`, gerado
por `_anatel_blf_flow(table_id, cron)`. O `microdados` tem cron diário; as 3 densidades
vão com `cron=None` (sem schedule — ver seção Densidades). Todos delegam a
`_run_anatel_banda_larga_fixa` (`pipelines/crawler/anatel/banda_larga_fixa/flows.py`).

### Parâmetros de execução

| Parâmetro                | Default  | Observação                                                                |
| ------------------------ | -------- | ------------------------------------------------------------------------- |
| `ano`                    | `None`   | Ano a processar; `None` deriva o ano corrente do ZIP                      |
| `target`                 | `"prod"` | Use `"dev"` para testes                                                   |
| `materialize_after_dump` | `True`   | Quando `True`, escreve no bucket `basedosdados` (prod) e materializa prod |
| `update_metadata`        | `True`   | Grava o Update de fonte (poll deferido) após materializar                 |
| `force_run`              | `False`  | `True` ignora o poll e força o dump/materialização                        |

> **Testes em dev sem tocar prod:** rode com `target="dev"`,
> `materialize_after_dump=False`, `force_run=True`. Com os defaults
> (`target="prod"`, `materialize_after_dump=True`), o flow grava no bucket de
> **produção** mesmo rodando no worker de dev.

---

## Histórico de Correções

- **Destravamento do microdados (PR #1635, merged):** substituição do poll eager
  (`register_source_poll_task`) pelo par deferido (`poll_source_for_update_task` +
  `commit_source_update_task`), que reescreve o `Update` envenenado (`2026-06-04`) por
  uma data de cobertura e só grava após materializar. No mesmo PR: extensão do range de
  partição do `microdados` (`end` 2023 → 2031) e **desativação do schedule das 3
  densidades** (`cron=None`) para evitar regressão (ver seção Densidades). Recuperação do
  backlog do microdados (maio/2026 + revisões) via `force_run`.
- **Pendente — tratamento das densidades (PR de follow-up):** fixes de `_parse_densidade`
  (NaN→NULL), `dtype=str` (corrige `id_municipio` `"…0"` que reprovava o `relationships`)
  e `dropna(["Código IBGE"])` (remove linhas sem código que reprovavam o
  `unique_combination`). Em espera junto da decisão sobre o apagão da fonte (PRUV).
  Rastreado na issue #1608.

> O `telefonia_movel` (`pipelines/crawler/anatel/telefonia_movel/flows.py`) tem o
> **mesmo** poll eager — o mesmo fix se aplica lá, em correção à parte.
