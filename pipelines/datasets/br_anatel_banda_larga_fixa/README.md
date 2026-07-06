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

## Revisão histórica da fonte (verificação 2026-07-06)

Baixando o ZIP e comparando a contagem de linhas por mês com o prod (script
`task_davi/verifica_fonte_blf.py`), observou-se que a Anatel **revisa meses passados
para cima** conforme chegam registros atrasados — e que a fonte já havia publicado
**maio/2026** enquanto o prod estava preso em abril:

| mês     | fonte  | BQ (prod) | diff          |
| ------- | ------ | --------- | ------------- |
| 2026-01 | 698274 | 696371    | +1903         |
| 2026-02 | 698074 | 692951    | +5123         |
| 2026-03 | 679799 | 671575    | +8224         |
| 2026-04 | 684785 | 669968    | +14817        |
| 2026-05 | 652876 | —         | novo na fonte |

Os diffs positivos crescem com a recência do mês (assinatura de revisão histórica). Isso
tem uma consequência prática importante: como o upload é `append` e o model dbt é
`create or replace` a partir do staging, o `force_run` **re-materializa do staging
revisado e realinha sozinho** — o backfill é idempotente por partição e, portanto,
seguro de reexecutar.

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
27 (UF) / 5570 (município) por mês. Durante o congelamento, as densidades `uf` e
`municipio` ficaram **1 mês atrás** do microdados (paradas em março enquanto a fonte já
tinha abril); o fix realinha na próxima materialização.

---

## Estrutura dos Flows

Um flow por tabela em `pipelines/datasets/br_anatel_banda_larga_fixa/flows.py`, gerado
por `_anatel_blf_flow(table_id, cron)` e agendado em horários distintos. Todos delegam
a `_run_anatel_banda_larga_fixa` (`pipelines/crawler/anatel/banda_larga_fixa/flows.py`).

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

- **Destravamento da atualização (PR #____):** substituição do poll eager
  (`register_source_poll_task`) pelo par deferido (`poll_source_for_update_task` +
  `commit_source_update_task`), que reescreve o `Update` envenenado (`2026-06-04`) por
  uma data de cobertura e só grava após materializar. No mesmo PR, extensão do range de
  partição do `microdados` (`end` 2023 → 2031). Recuperação do backlog (maio/2026 +
  revisões dos meses anteriores) via `force_run` por tabela.

> O `telefonia_movel` (`pipelines/crawler/anatel/telefonia_movel/flows.py`) tem o
> **mesmo** poll eager — o mesmo fix se aplica lá, em correção à parte.
