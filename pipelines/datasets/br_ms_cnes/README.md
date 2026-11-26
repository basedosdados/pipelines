# Documentação do Conjunto de Dados: CNES (Cadastro Nacional de Estabelecimentos de Saúde)

Este documento registra o contexto, os problemas conhecidos e as decisões tomadas
no conjunto `br_ms_cnes`, para quem for mantê-lo depois.

---

## Sobre o Sistema

O CNES é o cadastro oficial dos estabelecimentos de saúde do país, mantido pelo
DATASUS. Os dados são publicados mensalmente no FTP público, em arquivos `.dbc`
por grupo e por UF, sob `dissemin/publicos/CNES/200508_/Dados/<GRUPO>/`.

- [Portal CNES](https://cnes.datasus.gov.br/)
- [FTP DATASUS](ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/)
- Tabelas auxiliares: `TAB_CNES.zip`, em `dissemin/publicos/CNES/200508_/Auxiliar` (ver a
  seção do dicionário)

O nome do arquivo carrega a competência em `YYMM` nas posições 4–8 do basename:
`STAC2606.dbc` é o grupo ST, UF AC, competência 2026-06.

---

## Tabelas e particularidades

### br_ms_cnes__equipamento

`indicador_equipamento_disponivel_sus` e `indicador_equipamento_indisponivel_sus` são STRING
cobertos pelo dicionário. Só assumem `'0'` e `'1'` e são exatamente complementares: uma é a
negação da outra. O `Equipamento.def` do `TAB_CNES.zip` documenta "somente 1=SIM ou 0=NÃO".

### br_ms_cnes__estabelecimento_ensino

Parada em 2019-12 porque o DATASUS descontinuou o grupo EE. Não tem cron.

### br_ms_cnes__regra_contratual

Desativada. A leitura do CSV da fonte falha:

```text
ParserError: Error tokenizing data. C error: Expected 31 fields in line 213, saw 32
```

A issue #1703 foi fechada como `NOT_PLANNED`.

---

## Valores ausentes

Nos arquivos mais antigos da staging, o valor ausente está gravado como o texto `nan`, efeito
de um `astype(str)` no tratamento. O tratamento atual grava nulo. Os modelos leem a staging pelo
macro `cnes_staging_nan_as_null` (`macros/cnes_columns_standartization.sql`), que troca `nan` por
nulo em toda coluna de texto, menos `ano`, `mes` e `sigla_uf`: são as partições da staging e
ficam de fora para não perder a poda. O macro troca só `nan`, e o texto em branco passa como
está, porque vários modelos convertem `''` no código `'0'`.

Três colunas usam zeros para valor ausente, e os modelos as convertem em nulo: `cpf_cnpj` e
`cnpj_mantenedora` (14 zeros) no `estabelecimento` e `id_municipio_6_residencia` (6 zeros) no
`profissional`.

---

## Colunas fora do teste de nulos

O `not_null_proportion_multiple_columns` de cada modelo exige pelo menos 99% de valores
preenchidos em cada coluna, na competência mais recente. As colunas abaixo ficam fora dele
(`ignore_values`). Os percentuais foram medidos em prod em 23/11/2026, na competência 2026-10,
com o texto `nan` e o texto vazio contados como nulo.

Para conferir os valores de uma tabela, troque `estabelecimento` nos dois lugares da consulta.
Ela devolve a porcentagem de nulos de cada coluna na competência mais recente; coluna sem nenhum
nulo não aparece. A consulta lê todas as colunas do ano mais recente: uns 7 GiB no
`estabelecimento` e 11 GiB no `profissional`.

```sql
with ultima_competencia as (
  select *
  from `basedosdados.br_ms_cnes.estabelecimento`
  where date(ano, mes, 1) = (
    select max(date(ano, mes, 1)) from `basedosdados.br_ms_cnes.estabelecimento`
  )
)
select
  coluna,
  round(100 * count(*) / (select count(*) from ultima_competencia), 2) as percentual_nulos
from ultima_competencia as t,
  unnest(regexp_extract_all(to_json_string(t), r'"(\w+)":null')) as coluna
group by coluna
order by percentual_nulos desc
```

Colunas vazias. As marcadas com `*` estão vazias em todas as competências de 2024-01 a 2026-10;
as outras foram medidas só em 2026-10.

| Tabela | Coluna | Nulos |
|---|---|---|
| `dados_complementares` | `cns_medico_responsavel_administrador_responsavel_tecnico` | 99,74% |
| `dados_complementares` | `cns_medico_responsavel_oncologista_pediatrico` | 99,95% |
| `dados_complementares` | `cns_medico_responsavel_cirurgia_oncologica` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_oncologista_clinico` | 99,74% |
| `dados_complementares` | `cns_medico_responsavel_radioterapeuta` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_fisico_nuclear` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_nefrologista` | 100% |
| `estabelecimento` | `ano_avaliacao_pnass` `*` | 100% |
| `estabelecimento` | `mes_avaliacao_pnass` `*` | 100% |
| `estabelecimento` | `data_publicacao_contrato_estadual` `*` | 100% |
| `estabelecimento` | `data_publicacao_contrato_municipal` `*` | 100% |
| `estabelecimento` | `tipo_avaliacao_acreditacao_hospitalar` `*` | 100% |
| `estabelecimento` | `tipo_classificacao_acreditacao_hospitalar` `*` | 100% |
| `estabelecimento` | `tipo_natureza_administrativa` `*` | 100% |
| `estabelecimento` | `tipo_retencao_tributos` `*` | 100% |
| `estabelecimento` | `tipo_retencao_tributos_mantenedora` `*` | 100% |
| `estabelecimento` | `id_microrregiao_saude` | 100% |
| `estabelecimento` | `id_distrito_administrativo` | 100% |
| `estabelecimento` | `tipo_nivel_hierarquia` | 100% |
| `estabelecimento` | `id_contrato_municipio_sus` | 100% |
| `estabelecimento` | `id_contrato_estado_sus` | 100% |
| `profissional` | `cbo_1994` `*` | 100% |
| `profissional` | `indicador_estabelecimento_terceiro` `*` | 100% |
| `profissional` | `id_municipio_6_residencia` | 99,98% |
| `servico_especializado` | `quantidade_nacional_estabelecimento_saude_terceiro` `*` | 100% |

Colunas parcialmente preenchidas. Os nulos andam em grupos de colunas ligadas entre si: `banco`,
`agencia` e `conta_corrente` têm o mesmo número, assim como as duas do conselho e as duas do
serviço único.

| Tabela | Coluna | Nulos |
|---|---|---|
| `dados_complementares` | `cns_medico_responsavel_hemoterapeuta` | 54,92% |
| `dados_complementares` | `cns_medico_capacitado_responsavel` | 55,56% |
| `dados_complementares` | `cns_medico_responsavel_hematologista` | 66,56% |
| `dados_complementares` | `cns_responsavel_tecnico_sorologia` | 71,56% |
| `estabelecimento` | `cpf_cnpj` | 20,32% |
| `estabelecimento` | `tipo_orgao_expedidor` | 34,76% |
| `estabelecimento` | `numero_alvara` | 35,55% |
| `estabelecimento` | `data_expedicao_alvara` | 35,59% |
| `estabelecimento` | `id_regiao_saude` | 60,80% |
| `estabelecimento` | `cnpj_mantenedora` | 79,33% |
| `estabelecimento` | `banco` | 91,96% |
| `estabelecimento` | `agencia` | 91,96% |
| `estabelecimento` | `conta_corrente` | 91,96% |
| `estabelecimento` | `id_distrito_sanitario` | 92,39% |
| `profissional` | `id_registro_conselho` | 26,79% |
| `profissional` | `tipo_conselho` | 26,92% |
| `servico_especializado` | `tipo_servico_especializado_unico` | 47,11% |
| `servico_especializado` | `indicador_servico_especializado_unico` | 47,11% |

Colunas com poucos nulos, mas acima do 1% que o teste aceita:

| Tabela | Coluna | Nulos |
|---|---|---|
| `equipe` | `area` | 2,14% |
| `equipe` | `descricao_segmento` | 2,14% |
| `estabelecimento` | `tipo_fluxo_atendimento` | 1,44% |
| `habilitacao` | `portaria` | 1,49% |

---

## Particionamento

Linhas com `ano` fora do range de partição caem em `__UNPARTITIONED__`, e filtrar por `ano`
não poda nada nessa partição. No particionamento por inteiro do BigQuery o range é
`[start, end)` — o fim é exclusivo, e `"end": 2024` já deixa 2024 de fora. A convenção da BD é
`end = último ano + 5`, o que dá 2031 para dados até 2026.

| Modelo | `start` | `end` |
|---|---|---|
| `dados_complementares`, `equipamento`, `equipe`, `estabelecimento`, `estabelecimento_filantropico`, `gestao_metas`, `habilitacao`, `incentivos`, `leito`, `profissional`, `servico_especializado` | 2005 | 2031 |
| `estabelecimento_ensino`, `regra_contratual` | 2005 | 2024 |

`estabelecimento_ensino` e `regra_contratual` ficam em 2024: nenhuma linha está em
`__UNPARTITIONED__` e não entra dado novo nas duas (ver "Tabelas e particularidades").

**Alterar o config não reparticiona.** O particionamento é fixado quando a tabela é criada, e
só um `--full-refresh` aplica o range novo. O dbt não acusa a divergência: um `dbt run`
incremental com config novo sobre tabela velha passa normalmente.

### Backfill

O `extract_and_load` de cada tabela (`extract_and_load: br_ms_cnes__<tabela>`) aceita uma
lista de competências no formato `AAAAMM`:

```json
{"download_params": {"year_months": ["202609", "202610"]}}
```

Ele baixa do FTP os arquivos dessas competências, sobe para a staging e dispara o
`build_and_promote`. Competência sem arquivo no FTP faz a carga falhar antes do download.

O modelo é incremental e só insere competência maior que a máxima da tabela. Uma competência
que a tabela já tem, ou anterior a ela, só entra com um `--full-refresh` do modelo depois do
backfill.

---

## Agendamento

Crons em `America/Sao_Paulo`, declarados em `flows.py`:

| Tabela | Cron |
|---|---|
| estabelecimento | `0 9 * * *` |
| equipe | `30 9 * * *` |
| leito | `0 10 * * *` |
| equipamento | `30 10 * * *` |
| profissional | `40 10 * * *` |
| dados_complementares | `0 11 * * *` |
| estabelecimento_filantropico | `15 11 * * *` |
| gestao_metas | `30 11 * * *` |
| habilitacao | `45 11 * * *` |
| incentivos | `50 11 * * *` |
| servico_especializado | `30 12 * * *` |
| estabelecimento_ensino | sem cron |
| regra_contratual | sem cron |

`estabelecimento` roda antes de `leito` e `profissional` porque os dois buscam nele o
`id_municipio` da mesma competência.

---

## O dicionário

### Como funciona

#### Origem

O dicionário deste conjunto não é gerado por nada: não há crawler nem fonte registrada no
backend, e é a única tabela do conjunto sem flow. É um CSV único, mantido à mão, em
`gs://basedosdados-dev/staging/br_ms_cnes/dicionario/dicionario.csv`.

#### Como atualizar

Use `models/br_ms_cnes/code/update_dicionario.py`. Ele lê o staging, acrescenta as linhas
pendentes (ignorando as que já existem, pela chave `id_tabela + nome_coluna + chave`) e
regrava o CSV. São cinco estruturas, todas idempotentes:

| Estrutura | Para quê |
|---|---|
| `NEW_ROWS` | chaves novas de qualquer tabela ou coluna; traz `id_tabela` e `nome_coluna` por linha |
| `CODIGO_EQUIPAMENTO` | os 141 rótulos do código de 4 dígitos; só chave e valor, o resto é preenchido por `candidate_rows()` |
| `SIGTAP_LABELS` | rótulos da Tabela Unificada do SIGTAP para as colunas da `habilitacao` e do `servico_especializado`; só chave e valor, por coluna, como o `CODIGO_EQUIPAMENTO` |
| `CODES_WITHOUT_MEANING` | códigos sem significado nas fontes oficiais, por coluna; recebem o rótulo de `NO_MEANING_LABEL` |
| `REPLACE_ROWS` | reescreve o valor de linhas que já existem |

```bash
cd models/br_ms_cnes/code
export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/credentials-dev.json"
uv run --project ../../.. python update_dicionario.py --dry-run
uv run --project ../../.. python update_dicionario.py --apply

cd ../../..
uv run dbt run  --select br_ms_cnes__dicionario
uv run dbt test --select br_ms_cnes__equipamento br_ms_cnes__equipe
```

#### Como chega em prod

O script escreve só o bucket de dev. Quem leva o dicionário para prod é a action
`table-approve`, que copia `gs://basedosdados-dev/staging/<ds>/<tabela>` para
`gs://basedosdados/staging/<ds>/<tabela>` (com backup em `basedosdados-backup`), recria a
staging e roda o dbt em prod. Para isso:

1. a PR precisa carregar o rótulo `table-approve`;
2. a PR precisa alterar `models/br_ms_cnes/br_ms_cnes__dicionario.sql`, porque a action monta
   o par (conjunto, tabela) a partir dos `.sql` modificados.

A cópia apaga o destino antes. Confira que o staging de prod não tem nada que o de dev não
tenha.

#### Convenção de rótulo

O arquivo é inconsistente por acúmulo histórico: as chaves 1–8 de `tipo_equipamento` são
minúsculas e sem acento; as 9 e 10 são Title Case; os rótulos de `tipo_equipe` seguem o padrão
`sigla - descrição` em minúscula. As chaves novas usam Title Case com acento. O legado não foi
mexido.

Os rótulos tirados do SIGTAP e das portarias dos incentivos mantêm a grafia da fonte
(`CENTRO DE CONVIVÊNCIA BÁSICO`, `TELEDIAGNOSTICO`). Código sem significado nas fontes
oficiais leva o rótulo `Código <chave>: sem significado nas fontes oficiais (nota da BD)`,
como pede o manual de código sem rótulo, e entra no pedido via LAI de "Pendências".

### Mudanças no dicionário

As chaves 77 a 81 de `tipo_equipe` já existiam com o valor provisório "Não encontrado nos
dicionários oficiais" e ganharam o rótulo da norma; a `16` de `tipo_unidade` trocou o valor
provisório pelo rótulo de código sem significado. As outras linhas são chaves novas.

| PR | Coluna | Chave | Rótulo | Fonte |
|---|---|---|---|---|
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 11 | Avaliação Antropométrica e Funcional | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 12 | Radioterapia | Portaria SAES/MS nº 3.695/2026, Art. 2º §1º |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 13 | Quimioterapia | Portaria SAES/MS nº 3.695/2026, Art. 2º §1º |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 14 | Reabilitação | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 15 | Procedimentos Clínicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 16 | Procedimentos Cirúrgicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.codigo_equipamento` | 141 códigos | ver "Particularidades por coluna" | `CNV/Equip_Tp.cnv`, Portaria SAES/MS nº 3.695/2026 e legado |
| #1721 (31/07/2026) | `equipamento.indicador_equipamento_disponivel_sus` | 0, 1 | Não, Sim | `Equipamento.def` do `TAB_CNES.zip` |
| #1721 (31/07/2026) | `equipamento.indicador_equipamento_indisponivel_sus` | 0, 1 | Não, Sim | `Equipamento.def` do `TAB_CNES.zip` |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 77 | EMAP-R - Equipe Multiprofissional de Apoio para Reabilitação | Portaria SAES/MS nº 1.619/2024, Art. 3º §4º |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 78 | EACP - Equipe Assistencial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 79 | EMCP - Equipe Matricial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 80 | EAP-DESINST - Equipe de Avaliação e Acompanhamento de Medidas Terapêuticas (transtorno mental em conflito com a lei) | Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº 2.070/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 81 | EqAE - Equipe de Atenção Especializada | Portaria SAES/MS nº 3.200/2025, Art. 5º |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 82 | E-DOT - Equipe Hospitalar de Doação para Transplantes | CNES 4.8.40, via terceiros |
| #2320 (18/11/2026) | `equipe.tipo_equipe` | 83 | EMCP-Ped - Equipe Macrorregional de Cuidados Paliativos com Pediatra | Portaria SAES/MS nº 4.288/2026, Anexo II, via Conass Informa 140/2026 |
| #2331 | `leito.tipo_especialidade_leito` | 96 | Suporte Ventilatório Pulmonar | Portaria SAES/MS nº 550/2023, via [Conass Informa nº 94/2023](https://www.conass.org.br/conass-informa-n-94-2023-republicada-a-portaria-saes-n-550-que-reativa-o-leito-96-suporte-ventilatorio-pulmonar-no-cadastro-nacional-de-estabelecimentos-de-saude-cnes-e-inclui-pro/) |
| #2358 (26/11/2026) | `estabelecimento.tipo_unidade` | 16 | Código 16: sem significado nas fontes oficiais (nota da BD) | nenhuma; pedido via LAI ([#2367](https://github.com/basedosdados/pipelines/issues/2367)), ver "Particularidades por coluna" |
| #2358 (26/11/2026) | `habilitacao.tipo_habilitacao` | 72 códigos | ver abaixo | `tb_habilitacao` do SIGTAP, competência 2026-10 |
| #2358 (26/11/2026) | `incentivos.tipo_habilitacao` | 38 códigos | ver abaixo | portaria que criou cada incentivo |
| #2358 (26/11/2026) | `servico_especializado.tipo_servico_especializado` | 171, 172, 175 | ver abaixo | `tb_servico` do SIGTAP, competência 2026-10 |
| #2358 (26/11/2026) | `servico_especializado.tipo_servico_especializado_unico` | 171, 172, 175 | ver abaixo | `tb_servico` do SIGTAP, competência 2026-10 |
| #2358 (26/11/2026) | `servico_especializado.subtipo_servico_especializado` | 36 códigos | ver abaixo | `tb_servico_classificacao` do SIGTAP, competência 2026-10; o `170002` sem significado |

As tabelas abaixo detalham os códigos da #2358, um por linha. "Primeira competência" e
"Linhas" contam a tabela publicada até a competência 2026-10.

#### `habilitacao.tipo_habilitacao` (72)

Rótulos da `tb_habilitacao` da Tabela Unificada do SIGTAP, competência 2026-10
(`TabelaUnificada_202610_v2610271043.zip`), com o código sem o zero à esquerda.

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `406` | Serviço de Especialidade em Saúde Bucal | 2024-02 | 4.186 |
| `638` | CENTRO DE CONVIVÊNCIA BÁSICO | 2026-07 | 4 |
| `639` | CENTRO DE CONVIVÊNCIA INTERMEDIÁRIO | 2026-07 | 8 |
| `640` | CENTRO DE CONVIVÊNCIA AMPLIADO | 2026-07 | 20 |
| `815` | Implante Percutâneo de Válvula Aórtica (TAVI) | 2024-04 | 830 |
| `1420` | Hospital de Referência à Gestação e ao Puerpério de Alto Risco -HGPAR | 2025-08 | 3.360 |
| `1515` | Incremento financeiro à Terapia Renal Substitutiva - TRS | 2026-05 | 618 |
| `1618` | Neurocirurgia Endovascular/Trombectomia Mecanica | 2023-12 | 556 |
| `1722` | Tratamentos Integrados Sincrônicos em Oncologia | 2022-08 | 647 |
| `2308` | Serviço de Referência em Tratamento da Pessoa com Falência Intestinal | 2024-11 | 120 |
| `2436` | Incremento Financeiro SNT TMO Nível A | 2023-09 | 516 |
| `2437` | Incremento Financeiro SNT TMO Nível B | 2023-09 | 76 |
| `2438` | Incremento Financeiro SNT TMO Nível C | 2023-09 | 25 |
| `2439` | Incremento Financeiro SNT TMO Nível D | 2023-09 | 190 |
| `2440` | Incremento Financeiro SNT TMO Nível E | 2023-09 | 194 |
| `2441` | Incremento Financeiro SNT Pâncreas Nível A | 2023-09 | 126 |
| `2442` | Incremento Financeiro SNT Pâncreas Nível B | 2023-09 | 123 |
| `2443` | Incremento Financeiro SNT Pâncreas Nível C | 2023-09 | 131 |
| `2446` | Incremento Financeiro SNT Rim Nível A | 2023-09 | 1.624 |
| `2447` | Incremento Financeiro SNT Rim Nível B | 2023-09 | 547 |
| `2448` | Incremento Financeiro SNT Rim Nível C | 2023-09 | 635 |
| `2449` | Incremento Financeiro SNT Rim Nível D | 2023-09 | 154 |
| `2450` | Incremento Financeiro SNT Rim Nível E | 2023-09 | 111 |
| `2451` | Incremento Financeiro SNT Fígado Nível A | 2023-09 | 816 |
| `2452` | Incremento Financeiro SNT Fígado Nível B | 2023-09 | 307 |
| `2453` | Incremento Financeiro SNT Fígado Nível C | 2023-09 | 274 |
| `2454` | Incremento Financeiro SNT Fígado Nível D | 2023-09 | 215 |
| `2455` | Incremento Financeiro SNT Fígado Nível E | 2023-09 | 193 |
| `2456` | Incremento Financeiro SNT Pulmão Nível A | 2023-09 | 96 |
| `2458` | Incremento Financeiro SNT Pulmão Nível C | 2023-09 | 32 |
| `2461` | Incremento Financeiro SNT Coração Nível A | 2023-09 | 526 |
| `2462` | Incremento Financeiro SNT Coração Nível B | 2023-09 | 106 |
| `2463` | Incremento Financeiro SNT Coração Nível C | 2023-09 | 176 |
| `2464` | Incremento Financeiro SNT Coração Nível D | 2026-08 | 3 |
| `2465` | Incremento Financeiro SNT Coração Nível E | 2023-09 | 38 |
| `2466` | Transplante de intestino delgado ou multivisceral | 2025-06 | 96 |
| `2467` | Transplante de Membrana Amniótica | 2025-12 | 64 |
| `2468` | Banco de Multitecidos | 2025-10 | 148 |
| `2469` | Transplante Pediátrico Tipo I | 2025-10 | 307 |
| `2470` | Transplante Pediátrico Tipo II | 2025-10 | 236 |
| `2471` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado com HLA haploidêntico | 2026-01 | 58 |
| `2472` | Transplante de Células Progenitoras Hematopoéticas Autólogo Pediátrico I | 2026-10 | 15 |
| `2473` | Transplante de Células Progenitoras Hematopoéticas Autólogo Pediátrico II | 2026-10 | 9 |
| `2474` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Idêntico Pediátrico I | 2026-10 | 15 |
| `2475` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Idêntico Ped II | 2026-10 | 7 |
| `2476` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Haploidêntico - Ped I | 2026-10 | 3 |
| `2477` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Haploidêntico Ped II | 2026-10 | 2 |
| `2478` | Transplante de Células Progenitoras Hematopoéticas Não Aparentado Pediátrico I | 2026-10 | 14 |
| `2479` | Transplante de Células Progenitoras Hematopoéticas Não Aparentado Pediátrico II | 2026-10 | 7 |
| `2482` | Transplante de Rim - Pediátrico Tipo I | 2026-10 | 13 |
| `2483` | Transplante de Rim - Pediátrico Tipo II | 2026-10 | 15 |
| `2484` | Transplante de Fígado - Pediátrico Tipo I | 2026-10 | 13 |
| `2485` | Transplante de Fígado - Pediátrico Tipo II | 2026-10 | 9 |
| `2486` | Transplante de Pulmão - Pediátrico Tipo I | 2026-10 | 2 |
| `2487` | Transplante de Pulmão - Pediátrico Tipo II | 2026-10 | 2 |
| `2488` | Transplante de Coração - Pediátrico Tipo I | 2026-10 | 2 |
| `2489` | Transplante de Coração - Pediátrico Tipo II | 2026-10 | 4 |
| `2490` | Transplante de Intestino Delgado ou Multivisceral - Pediátrico Tipo I | 2026-10 | 1 |
| `2902` | Programa Mais Acesso a Especialistas - Componente Cirurgias | 2023-09 | 41.636 |
| `3516` | Terapia Génica | 2025-10 | 103 |
| `3801` | Programa Mais Acesso a Especialistas | 2024-06 | 68.695 |
| `3803` | Agora Tem Especialistas Modalidade 2 Equipes Volantes | 2025-12 | 167 |
| `3804` | Agora Tem Especialistas Modalidade 3 Unidades Móveis | 2025-09 | 1.044 |
| `3805` | Agora Tem Especialistas - Componente Créditos Financeiro | 2025-10 | 1.470 |
| `3806` | Agora Tem Especialistas - Componente Ressarcimento ao SUS | 2025-11 | 60 |
| `3807` | Componente Acesso à Radioterapia do Programa Agora Tem Especialistas | 2025-11 | 2.520 |
| `3808` | Agora Tem Especialistas - Teleatendimento em Saúde Mental | 2026-10 | 1 |
| `3811` | Agora tem Especialistas 3 Unidade Móvel Exame de Imagem | 2026-10 | 56 |
| `3812` | Agora tem Especialistas 3 Unidade Móvel Oftalmologia | 2026-10 | 19 |
| `3813` | Agora tem Especialistas 3 Unidade Móvel Saúde da Mulher | 2026-10 | 66 |
| `3814` | Agora tem Especialistas 3 Unidade Móvel Cardiologia | 2026-10 | 4 |
| `3815` | Agora tem Especialistas 3 Unidade Móvel Saúde Bucal | 2026-10 | 3 |

#### `incentivos.tipo_habilitacao` (38)

Rótulos com o nome do ato que criou cada incentivo. Nenhum deles está no SIGTAP, cuja
`tb_habilitacao` não tem código começando com 8.

| Chave | Rótulo | Primeira competência | Linhas | Ato |
|---|---|---|---|---|
| `8116` | DRC Categoria I | 2023-12 | 10.062 | Portaria GM/MS nº 1.992/2023, Art. 1º |
| `8117` | DRC Categoria II | 2023-12 | 7.015 | Portaria GM/MS nº 1.992/2023, Art. 1º |
| `8119` | Hospital Sem Fins Lucrativos 100% SUS | 2023-08 | 4.541 | Portaria SAES/MS nº 602/2023, Art. 1º |
| `8121` | Qualificação em Doação e Transplantes - IFQ-DOT I | 2026-06 | 49 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8122` | Qualificação em Doação e Transplantes - IFQ-DOT II | 2026-07 | 15 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8123` | Qualificação em Doação e Transplantes - IFQ-DOT III | 2026-06 | 25 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8124` | Qualificação em Doação e Transplantes - IFQ-DOT IV | 2026-06 | 29 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8125` | Equipes de Apoio Assistencial em Cuidados Paliativos | 2026-08 | 36 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8126` | Equipes Macrorregionais de Cuidados Paliativos | 2026-08 | 3 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8127` | Equipes Macrorregionais de Cuidados Paliativos com Pediatra | 2026-08 | 3 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8256` | UPA 24h Nova - Qualificada Opção I | 2023-10 | 59 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8257` | UPA 24h Nova - Qualificada Opção II | 2018-02 | 147 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8258` | UPA 24h Nova - Qualificada Opção IV | 2019-07 | 538 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8259` | UPA 24h Nova - Qualificada Opção VI | 2018-07 | 413 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8260` | UPA 24h Nova - Qualificada Opção VII | 2018-01 | 624 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8263` | UPA 24h Ampliada - Habilitada/Qualificada Opção IV | 2023-03 | 112 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8264` | UPA 24h Ampliada - Habilitada/Qualificada Opção VI | 2023-11 | 72 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8266` | UPA 24h Nova - Habilitada Opção I | 2017-12 | 791 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8267` | UPA 24h Nova - Habilitada Opção II | 2017-12 | 770 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8268` | UPA 24h Nova - Habilitada Opção IV | 2018-09 | 787 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8269` | UPA 24h Nova - Habilitada Opção VI | 2018-01 | 782 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8270` | UPA 24h Nova - Habilitada Opção VII | 2017-12 | 672 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8271` | Enfermaria Clínica de Retaguarda - Novos | 2021-06 | 29.416 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8272` | Enfermaria Clínica de Retaguarda - Qualificados | 2021-06 | 26.188 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8273` | UTI Adulto RUE Tipo II - Novos | 2021-06 | 15.018 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8274` | UTI Adulto RUE Tipo II - Qualificados | 2021-06 | 36.920 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8275` | UTI Pediátrico RUE Tipo II - Novos | 2021-06 | 1.963 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8276` | UTI Pediátrico RUE Tipo II - Qualificados | 2021-06 | 9.832 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8277` | UTI Adulto RUE Tipo III - Novos | 2021-06 | 642 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8278` | UTI Adulto RUE Tipo III - Qualificados | 2021-06 | 2.690 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8279` | UTI Pediátrico RUE Tipo III - Novos | 2024-03 | 32 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8280` | UTI Pediátrico RUE Tipo III - Qualificados | 2021-06 | 1.974 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8285` | Atenção especializada ao seguimento do recém-nascido e crianças egressos de unidades neonatais (ANEO) | 2022-11 | 95 | Portaria SAES/MS nº 216/2022, Anexo II |
| `8286` | Transporte Sanitário Adaptado I | 2024-04 | 238 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8287` | Transporte Sanitário Adaptado II | 2024-09 | 105 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8288` | Transporte Sanitário Adaptado III | 2025-09 | 14 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8290` | Núcleo de Atenção à Criança e Adolescente com Transtorno do Espectro Autista | 2025-11 | 91 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8291` | Incentivo de 20% no custeio do CER habilitado com a modalidade de reabilitação intelectual | 2024-09 | 777 | Portaria SAES/MS nº 1.148/2023, Anexo IV |

- O `8285` foi excluído da tabela de incentivos pela Portaria SAES/MS nº 2.902/2025 (Art. 5º
  §2º), mas continua nos dados até 2026-10.
- Os números 81.16 a 81.39 já foram incentivos de equipes de saúde prisional (Portaria SAS/MS
  nº 305/2014, Anexo II), desativados pela Portaria SAES/MS nº 37/2021 (Art. 10). Nos dados,
  o `8116`, o `8117` e o `8119` só aparecem a partir de 2023, e os `8121` a `8127` a partir de
  2026, depois de os números serem reaproveitados. Por isso o rótulo atual vale para todas as
  linhas.

#### `servico_especializado.tipo_servico_especializado` (3)

Os rótulos das três colunas do `servico_especializado` vêm da Tabela Unificada do SIGTAP,
competência 2026-10: da `tb_servico` para o serviço e da `tb_servico_classificacao` para o
subtipo (código do serviço seguido do código da classificação). O `170002` não está no
SIGTAP: lá, o serviço 170 (Comissões e Comitês) só tem a classificação 001 (Núcleo de
Segurança do Paciente).

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `171` | Cuidados Paliativos | 2024-10 | 6.939 |
| `172` | Atenção ao Luto Materno e Parental | 2026-03 | 321 |
| `175` | Cirúrgia Robótica | 2026-08 | 24 |

#### `servico_especializado.tipo_servico_especializado_unico` (3)

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `171` | Cuidados Paliativos | 2024-10 | 3.708 |
| `172` | Atenção ao Luto Materno e Parental | 2026-03 | 321 |
| `175` | Cirúrgia Robótica | 2026-08 | 24 |

#### `servico_especializado.subtipo_servico_especializado` (36)

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `112007` | Ambulatório de Seguimento do RN e Criança Egresso de Unidade Neonatal | 2022-08 | 911 |
| `112008` | Ambulatório  de Gestação e Puerpério de Alto Risco | 2022-08 | 1.934 |
| `114008` | Odontopediatria | 2024-10 | 4.447 |
| `114009` | Disfunsão temporomandibular | 2024-10 | 459 |
| `114010` | Estomatologia | 2024-10 | 1.529 |
| `114011` | Implantodondia | 2024-10 | 4.249 |
| `114012` | Odontogeriatria | 2024-10 | 363 |
| `114013` | Ortodontia | 2024-10 | 4.826 |
| `114014` | Prótese Dentária | 2024-10 | 3.804 |
| `114015` | Radiologia Odontológica e Imaginologia | 2024-10 | 1.112 |
| `114016` | Acupuntura na Odontologia | 2024-10 | 1.018 |
| `114017` | Ortopedia funcional dos maxilares | 2024-10 | 673 |
| `114018` | Odontologia hospitalar | 2024-10 | 800 |
| `114019` | Patologia Oral e Máxilo Fácial | 2025-08 | 48 |
| `114020` | Prótese Buco Maxilo Facial | 2025-08 | 48 |
| `115010` | DESINST de Pessoas com Transtorno  Mental em Conflito com a Lei | 2024-10 | 811 |
| `115011` | Promoção da Saúde Mental, Protagonismo e Cidadania | 2026-02 | 2.033 |
| `125010` | Manipulação, Reconstituição e Diluição de Medicamentos | 2026-10 | 4 |
| `141005` | Vigilância de Zoonoses | 2023-06 | 16.407 |
| `154003` | Banco de Multitecidos | 2025-10 | 38 |
| `160004` | TELEDIAGNOSTICO | 2023-12 | 94.148 |
| `160005` | TELE-EDUCACAO | 2023-12 | 76.478 |
| `160006` | TELECONSULTA | 2023-12 | 440.453 |
| `160007` | TELEMONITORAMENTO | 2023-12 | 87.083 |
| `160008` | TELEORIENTACAO | 2023-12 | 100.296 |
| `160009` | TELEINTERCONSULTA | 2023-12 | 93.297 |
| `160010` | TELETRIAGEM | 2023-12 | 75.042 |
| `160011` | TELERREGULACAO | 2023-12 | 54.190 |
| `164012` | Confecção de OPM Auxiliares de Locomoção e Ortopédicas | 2024-11 | 691 |
| `168003` | Terapia Génica | 2025-11 | 47 |
| `170002` | Código 170002: sem significado nas fontes oficiais (nota da BD) | 2024-06 | 54.931 |
| `171001` | Abordagem Paliativa | 2024-10 | 3.637 |
| `171002` | Apoio Matricial em Cuidados Paliativos | 2024-10 | 2.280 |
| `171003` | Cuidados Paliativos Especializados | 2026-08 | 1.022 |
| `172001` | Assistência Multiprofissional ao luto materno e parental | 2026-03 | 321 |
| `175001` | Prostatectomia | 2026-08 | 24 |

#### Fontes dos rótulos

| Fonte | Códigos |
|---|---|
| Tabela Unificada do SIGTAP, competência 2026-10, em `ftp2.datasus.gov.br/pub/sistemas/tup/downloads/` | `habilitacao` e `servico_especializado` |
| [Portaria GM/MS nº 1.992, de 24/11/2023](https://bvsms.saude.gov.br/bvs/saudelegis/gm/2023/prt1992_28_11_2023.html) | `8116`, `8117` |
| [Portaria SAES/MS nº 602, de 28/07/2023](https://bvsms.saude.gov.br/bvs/saudelegis/Saes/2023/prt0602_02_08_2023.html) | `8119` |
| Portaria SAES/MS nº 4.020, de 10/04/2026, DOU de 12/05/2026, edição 87, seção 1, p. 161 ([Conass Informa 84/2026](https://www.conass.org.br/conass-informa-n-84-2026-publicada-a-portaria-saes-n-4-020-que-estabelece-regras-para-registro-das-equipes-hospitalares-de-doacao-para-transplantes-no-cadastro-nacional-de-estabelecimentos/)) | `8121` a `8124` |
| Portaria SAES/MS nº 4.288, de 22/06/2026, DOU de 23/07/2026, edição 137, seção 1, p. 339 ([Conass Informa 140/2026](https://www.conass.org.br/conass-informa-n-140-2026-publicada-a-portaria-saes-n-4-288-que-atualiza-o-cadastro-nacional-de-estabelecimentos-de-saude-cnes-e-a-tabela-de-procedimentos-medicamentos-orteses-pr/)) | `8125` a `8127` |
| [Portaria SAS/MS nº 1.535, de 25/09/2017](https://bvsms.saude.gov.br/bvs/saudelegis/sas/2017/prt1535_09_10_2017.html) | `8256` a `8260`, `8263`, `8264`, `8266` a `8270` |
| [Portaria SAES/MS nº 474, de 22/04/2021](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2021/prt0474_27_04_2021.html) | `8271` a `8280` |
| [Portaria SAES/MS nº 216, de 01/07/2022](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2022/prt0216_01_07_2022.html) | `8285` |
| [Portaria SAES/MS nº 2.902, de 26/06/2025](https://bvsms.saude.gov.br/bvs/saudelegis/Saes/2025/prt2902_02_07_2025.html) | exclusão do `8285` |
| [Portaria SAES/MS nº 1.148, de 21/12/2023](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2023/prt1148_22_12_2023.html) | `8286` a `8288`, `8290`, `8291` |
| [Portaria SAS/MS nº 305, de 10/04/2014, Anexo II](https://bvsms.saude.gov.br/bvs/saudelegis/sas/2014/anexo/anexo_prt0305_10_04_2014.pdf) e [Portaria SAES/MS nº 37, de 18/01/2021](https://www.in.gov.br/en/web/dou/-/portaria-n-37-de-18-de-janeiro-de-2021-299987615) | significado anterior de 81.16 a 81.39 |

Consultados sem os códigos desta seção: a `tb_habilitacao` e a `tb_grupo_habilitacao` do
SIGTAP de 2023-01, 2023-12, 2024-06, 2025-06 e 2026-10; o `TAB_CNES.zip` de 15/10/2026, que
não tem CNV de habilitação, serviço ou incentivo; as Portarias SAS/MS nº 461/2014, SAES/MS
nº 1.187/2023, 2.151/2024, 2.747/2025, 3.033/2025, 3.511/2025, 3.695/2026 e 4.399/2026 e
GM/MS nº 11.179/2026.

### Particularidades por coluna

#### `equipamento.tipo_equipamento`

Das chaves 11 a 16, só 12 e 13 têm fonte primária. As outras quatro vêm de terceiros citando a
Portaria 4.109/2026, cujo texto oficial não foi localizado. O `TP_EQUIPAM.dbf` do
`TAB_CNES.zip` traz só 10 tipos.

#### `equipamento.codigo_equipamento`

**O código de 4 dígitos.** O CNES identifica um equipamento por um código de 4 dígitos: os 2
primeiros são o tipo, os 2 últimos são o equipamento dentro daquele tipo. A numeração do
equipamento recomeça a cada tipo — existe um equipamento 01 na radiologia, outro na
telessaúde, outro na diálise. Só os 4 dígitos juntos identificam.

A fonte entrega as duas metades separadas (`TIPEQUIP` e `CODEQUIP`), que o modelo guarda em
`tipo_equipamento` e `id_equipamento`, e junta em `codigo_equipamento`. O dicionário sob
`id_equipamento` só serve para os tipos 1 a 8: o DATASUS embutia o tipo no primeiro dígito do
equipamento (tipo 2 usa 21–23, tipo 3 usa 31–50), o que fazia o número de 2 dígitos bastar até
o tipo 9, cuja faixa já estava ocupada pelo tipo 8. Para os demais, o rótulo certo está sob
`codigo_equipamento`.

O `tipequip` vem `'7'` no staging até 2025 e `'07'` depois; `codequip` tem sempre 2
caracteres. Por isso o modelo aplica `lpad` nas duas metades.

**De onde saem os rótulos dos 4 dígitos.** A série tem 207 códigos distintos:

| Procedência | Códigos | Linhas em 2026-06 |
|---|---|---|
| `CNV/Equip_Tp.cnv` | 94 | 969.154 |
| Portaria SAES/MS nº 3.695/2026, Anexo I | 30 | 56.318 |
| Legado transposto do dicionário | 17 | 70.702 |
| Sem rótulo | 66 | 1.681 |

O `CNV/` do `TAB_CNES.zip` é o dicionário oficial do DATASUS: formato de conversão do TabWin,
texto puro em latin-1, uma linha por equipamento com o código de 4 dígitos ao final.

A Portaria 3.695 tem precedência sobre o CNV, por ser a norma e mais recente. Ela criou 27
códigos que o CNV, regerado em 2026-07-21, não traz, e renomeia códigos existentes (`0107`
"Raio X Dentário" → "Raio X Odontológico", `0117` "Mamografo computadorizado" → "Mamógrafo
Digital"); o dicionário usa os nomes da portaria. Os códigos `0119` a `0135` vêm do Art. 2º
§2º, que reclassifica o `05 Raio X de 100 a 500mA` em `20 Raio X Analógico`, `21 Raio X
Digital`, `22 Raio X Telecomandado` e assim por diante; entram nos dados em 2026-03. O texto
oficial está no DOU de 18/05/2026, Edição 91, Seção 1, Página 174.

O legado é transposto por regra: se o número de 2 dígitos existe no dicionário e o tipo em
questão é o dominante daquele número na série, o rótulo vai para o código de 4 dígitos. Os 17
casos são coerentes com o tipo: o tipo 02 (infraestrutura) recebeu ar condicionado,
refrigerador, câmaras de conservação e geradores; o tipo 06 (outros) recebeu veículo
utilitário, embarcação e empilhadeira.

A grafia não é normalizada: cada rótulo vem como está na sua fonte. O CNV escreve
`Gama Câmara` onde o dicionário legado escrevia `gama camara`, e a Portaria 3.695 renomeia
coisas (`raio x dentario` → `Raio X Odontológico`). Por isso o mesmo equipamento aparece com
duas grafias no dicionário, uma sob `id_equipamento` e outra sob `codigo_equipamento`.

#### `equipe.tipo_equipe`

O texto oficial foi lido para 77 e 81. Para 78 e 79, a fonte é o Conass Informa 156/2024,
que resume o Anexo II. Para 80, nenhuma das duas portarias foi lida. O 82 vem de terceiros
citando o CNES 4.8.40. O `EQUIPE.dbf` do `TAB_CNES.zip` vai até o 76.

O nome que o próprio estabelecimento dá à equipe (coluna `equipe`, `nome_eqp` na fonte) bate
com a norma nos cinco casos de 77 a 81:

```text
77 → EMAP-R · MELHOR EM CASA · PMEC · EQUIPE DE APOIO A REALIBITACAO
78 → EACP · CUIDADOS PALIATIVOS · EQUIPE DE CUIDADOS PALIATIVOS
79 → EMCP - CUIDADOS PALIATIVOS · EQUIPE MATRICIAL DE CP
80 → EAP-DESINST ESTADUAL SAO LUIS · EAP - DESINST · SISTEMA PRISIONAL I
81 → AGORA TEM ESPECIALISTAS · JORNADA OFTALMOLOGICA · EQUIPE PRONAS-PCD
```

O rótulo do 80 é abreviado: o nome completo (`Equipe de Avaliação e Acompanhamento de
Medidas Terapêuticas Aplicáveis à Pessoa com Transtorno Mental em Conflito com a Lei`) tem
137 caracteres.

#### `estabelecimento.tipo_unidade`

A chave `16` aparece nos dados a partir de 2025-09, e nenhuma fonte do CNES diz o que ela
significa. O dicionário a registra com o rótulo `Código 16: sem significado nas fontes
oficiais (nota da BD)`, e o significado foi pedido ao Ministério da Saúde via LAI
([#2367](https://github.com/basedosdados/pipelines/issues/2367)).

| Fonte consultada | O que traz |
|---|---|
| `CNV/TP_ESTAB.CNV` do `TAB_CNES.zip`, versão de 15/10/2026 (`ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Auxiliar`) | Lista oficial dos tipos de estabelecimento. Pula do `15` (`UNIDADE MISTA`) para o `20` (`PRONTO SOCORRO GERAL`). O código é o número no fim da linha; o do início é só a posição |
| [CnesWeb, tipos de estabelecimento](https://cnes2.datasus.gov.br/Mod_Ind_Unidade.asp?VEstado=00) | A mesma lista, com a contagem por tipo, também sem o `16` |
| Ficha do estabelecimento no CnesWeb: `https://cnes2.datasus.gov.br/Exibe_Ficha_Estabelecimento.asp?VCo_Unidade=<6 primeiros dígitos do id_municipio><id_estabelecimento_cnes>` | Em seis estabelecimentos com `16` em 2026-10, os campos "Tipo Estabelecimento" e "Sub Tipo Estabelecimento" estão vazios. Na ficha de um estabelecimento de outro tipo (`3301700777994`, oficina ortopédica), o mesmo campo traz o nome do tipo |
| [Portaria GM/MS nº 2.022/2017](https://bvsms.saude.gov.br/bvs/saudelegis/gm/2017/prt2022_15_08_2017_rep.html) | Cria a nova classificação de tipos de estabelecimento, sem código numérico para os tipos |
| [Wiki do CNES, nova classificação de tipos](https://wiki.saude.gov.br/cnes/index.php/Categoria:Nova_Classifica%C3%A7%C3%A3o_de_Tipos_de_Estabelecimento) | O tipo é atribuído a partir das atividades informadas no cadastro. A tabela de códigos só aparece como imagem |
| [Ofício Circular nº 14/2026/CGSI/DRAC/SAES/MS](https://www.conass.org.br/wp-content/uploads/2026/09/9.1-Oficio-Circular-no-14.2026-CGSI-DRAC-SAES-MS-1.pdf) | A partir da competência 10/2026, o campo "Tipo de Estabelecimento" é substituído pela "Classificação de Tipo de Estabelecimento" |
| [Portaria SAES/MS nº 3.200/2025](https://ses.sp.bvs.br/wp-content/uploads/2025/09/U_PT-MS-SAES-3200_020925.pdf) | Exige a classificação "Ambulatório" com estrutura móvel para a habilitação 38.04, sem código numérico |

Competências em que o código aparece:

```sql
select date(ano, mes, 1) competencia, count(*) estabelecimentos
from `basedosdados.br_ms_cnes.estabelecimento`
where tipo_unidade = '16'
group by 1 order by 1
```

Resultado em prod, em 23/11/2026 (o código não aparece antes de 2025-09):

| Competência | Estabelecimentos |
|---|---|
| 2025-09 | 10 |
| 2025-10 | 21 |
| 2025-11 | 43 |
| 2025-12 | 54 |
| 2026-01 | 59 |
| 2026-02 | 86 |
| 2026-03 | 125 |
| 2026-04 | 149 |
| 2026-05 | 209 |
| 2026-06 | 256 |
| 2026-07 | 293 |
| 2026-08 | 328 |
| 2026-09 | 358 |
| 2026-10 | 148 |

#### `leito.tipo_especialidade_leito`

A chave `96` é o leito "Suporte Ventilatório Pulmonar", criado pela Portaria SAES/MS nº
510/2020 e reativado pela Portaria SAES/MS nº 550, de 03/07/2023. Ela não está no
`CNV/Esp_leit.CNV` do `TAB_CNES.zip` (versão de 15/10/2026), que termina no `95`.

### Pendências

- **Pedido via LAI ao Ministério da Saúde**, protocolo 25072.076527/2026-30
  ([#2367](https://github.com/basedosdados/pipelines/issues/2367)), enviado em
  23/11/2026 pelo Fala.BR (prazo de 20 dias, prorrogável por mais 10). Pergunta o
  significado do `estabelecimento.tipo_unidade` `16` e do
  `servico_especializado.subtipo_servico_especializado` `170002`, os dois com o rótulo
  `Código <chave>: sem significado nas fontes oficiais (nota da BD)`. O pedido também
  perguntava pelos 38 incentivos de `incentivos.tipo_habilitacao`, que depois foram
  achados nas portarias listadas em "Mudanças no dicionário". Quando a resposta chegar,
  trocar os dois rótulos pelo significado informado.
- **66 códigos de 4 dígitos sem rótulo**, 1.681 linhas em 2026-06: os tipos 11, 14, 15 e 16
  (37 códigos, 340 linhas), que dependem da Portaria 4.109, cujo texto oficial não foi
  localizado e que também não está no CNV, e 29 códigos antigos cujo tipo dominante não bate
  (`0351`, `0912`, `0265`, uma série de `077x`), de 20 a 480 linhas cada.
- **O texto oficial do `tipo_equipe` 80 não foi conferido.**
- **`cobertura_temporal` é `(1)` em todas as linhas**, em vez da notação `INICIO(1)FIM` do
  manual de estilo.
- **Encoding quebrado** em `id_equipamento` chave 97:
  `sistema completo de reforafaEUR!o visual(vra)`. O equivalente em `codigo_equipamento`
  (`0897`) está correto.
- **Significados repetidos** em `tipo_equipe`: as chaves 1 e 70 são ambas
  `esf - equipe de saude da familia`; 49 e 76 são ambas `eap`.
