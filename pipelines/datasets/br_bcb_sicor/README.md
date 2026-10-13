# Documentação do Conjunto de Dados: SICOR (Sistema de Operações do Crédito Rural e do Proagro)

Este documento registra informações importantes sobre a base de dados do SICOR, consolidando o contexto de problemas identificados e particularidades para futuros mantenedores.

---

## Sobre o Sistema

O SICOR é alimentado com dados enviados mensalmente pelas instituições financeiras integrantes do Sistema Nacional de Crédito Rural (SNCR).

- [Modelo de Entidade-Relacionamento do SICOR](https://www.bcb.gov.br/htms/sicor/manualDadosSicorCompleto.pdf)
- [Página de download de tabelas e dicionários](https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro)

## Particionamento no Storage

A fonte original divulga os dados seguindo quatro padrões de periodicidade:

1. Arquivos divulgados anualmente;
2. Arquivos divulgados anualmente, com ocorrências de arquivos semestrais;
3. Arquivos divulgados em períodos plurianuais;
4. Arquivos que são divulgados de forma única.

## Permissionamento e Estrutura de Vínculos

Este conjunto de dados possui uma tabela mestre: `br_bcb_sicor__operacao`.

- Esta tabela contém os dados cadastrais básicos de todas as operações de crédito financiadas com fontes públicas e privadas.
- O vínculo entre tabelas deve ser realizado via `id_referencia_bacen` e `numero_ordem`.
- Para permissionamento no BD PRO, utiliza-se as colunas `ano_emissao` e `mes_emissao` (adicionadas via macro `add_ano_mes_operacao_data` em quase todas as tabelas). Isso garante que usuários do plano gratuito tenham acesso a todos os dados referentes a um par de `id_referencia_bacen` e `numero_ordem` específico dentro das janelas permitidas.

## Lógica da materialização incremental das tabelas operacao, saldo e gleba;

A fonte original divulga esses dados em arquivos anuais. Para facilitar a lógica de atualização incremental, decidi manter esse padrão. Dessa forma, a estratégia utilizada não é append, mas insert_overwrite; a lógica é sobrescrever o ano máximo da tabela atualizada com o arquivo do ano atual que está sendo atualizado.

---

## Pipeline: armadilhas de atualização

Três coisas que já quebraram os flows deste conjunto e vão quebrar de novo se não forem lembradas.

### 1. O BCB mexe no schema da fonte, e isso quebra o flow de dois jeitos

Em 2026 aconteceram os dois casos, com meses de diferença: uma coluna **adicionada** em `saldo` e uma **renomeada** em `recurso_publico_propriedade`. Os sintomas e os consertos são diferentes.

#### 1a. Coluna adicionada, e o modo `append` não a absorve

`operacao`, `saldo` e `recurso_publico_gleba` sobem com `dump_mode="append"`. Nesse caminho, `upload_to_gcs` só cria a tabela de staging **se ela ainda não existir**; existindo, registra `Tabela já existe` e segue. Como a staging é **tabela externa com schema fixado na criação**, uma coluna nova na fonte não entra por conta própria — o parquet novo tem a coluna, a definição da tabela não.

Isso não é hipotético. Em julho de 2026 o BCB adicionou `IB_RENEGOCIADA` ao arquivo de saldos, e a mesma mudança quebrou o flow por dois caminhos diferentes:

- **antes** de registrar a coluna no repo, o `TableSchemaValidator` derrubava o flow (`columns in the source schema being ignorated`);
- **depois** de registrar (commit `0cfc1e6f`, coluna `indicador_renegociacao`), o dbt passou a quebrar com `Query error: Unrecognized name: indicador_renegociacao`, porque a staging continuava com o schema velho.

**Até julho de 2026** isso exigia intervenção manual: registrar a coluna em `constants.py`, no `.sql` e no `schema.yml` não bastava para as três tabelas em `append`, porque a definição da staging precisava ser atualizada à parte.

**Hoje é automático.** `_sync_staging_schema`, em `pipelines/utils/tasks.py`, roda no modo `append` quando a staging já existe, compara o schema inferido do arquivo novo com o da tabela externa e acrescenta o que faltar. É aditivo: nunca remove nem reordena coluna, e não toca no prefixo do GCS. Fique de olho no log `Colunas novas na fonte adicionadas ao schema da staging`, que é o sinal de que a fonte mudou.

Duas coisas continuam sendo responsabilidade de quem faz a manutenção, porque o ajuste não cobre: **registrar a coluna** em `constants.py`, `.sql` e `schema.yml` (sem isso o `TableSchemaValidator` derruba o flow), e o **`--full-refresh`** do item 3 abaixo.

#### 1b. Coluna renomeada ou removida — o ajuste não cobre, e não deveria

O caso espelho. Em 29/07/2026 o BCB republicou `SICOR_PROPRIEDADES` trocando `CD_NIRF` por `CD_CIB`, na mesma posição:

```text
antes:  #REF_BACEN;NU_ORDEM;CD_CNPJ_CPF;CD_SNCR;CD_NIRF;CD_CAR
depois: #REF_BACEN;NU_ORDEM;CD_CNPJ_CPF;CD_SNCR;CD_CIB;CD_CAR
```

O flow morre antes do upload, no `TableSchemaValidator`:

```text
The following columns are in the table schema registed in constants.py
but arent in the downloaded table {'CD_NIRF'}
```

`_sync_staging_schema` **não** resolve isso, por duas razões: ele é aditivo de propósito (uma carga parcial não pode encolher o schema de uma tabela histórica), e a falha acontece a montante, antes de qualquer upload.

O conserto é manual e são **quatro** lugares — esquecer o último faz o CI do repositório quebrar:

1. `constants.py` — o mapeamento `CD_CIB: id_cib`;
2. o `.sql` do modelo;
3. o `schema.yml` — a definição da coluna **e** qualquer teste que a cite (aqui ela estava na `unique_combination_of_columns`);
4. os **metadados de prod** — senão o check `Metadata validation (BigQuery vs API)` acusa a divergência. E como `update_column` não renomeia, é criar a nova e apagar a antiga.

**A ordem importa.** O check compara BigQuery de **dev** contra a API de **prod**. Mexer nos metadados antes de o modelo ter rodado em dev só inverte a divergência. O caminho é: código → run em dev (que recria a staging, já que essas tabelas são `overwrite`) → metadados de prod.

Antes de renomear, vale conferir se a coluna carregava dado. No caso do `id_nirf` não carregava: **zero valores preenchidos em 27 milhões de linhas, de 2013 a 2026** — a renomeação não teve efeito prático nenhum sobre o dado publicado.

### 2. Nunca use `overwrite` nem `delete_table` para consertar isso

Duas saídas parecem óbvias e são destrutivas:

- **Trocar o `dump_mode` para `"overwrite"`** — esse caminho faz `st.delete_table(mode="staging")` + `tb.delete(mode="all")`, e o flow baixa apenas o arquivo do ano corrente. Você perde o histórico da staging e fica só com o ano atual. Em `saldo`, isso significaria trocar ~641 milhões de linhas (2013–2026) por ~5,6 milhões (2026).
- **Chamar `Storage.delete_table(mode="staging")` avulso** — ele lista **todos** os blobs sob `staging/<dataset>/<tabela>/` e apaga. No caminho de `append` ele aparece logo após o `create`, mas ali é seguro porque o prefixo está vazio, contendo só o header recém-subido. Com a staging populada, apaga os dados.

O que é seguro: atualizar **apenas a definição** da tabela externa, sem tocar no prefixo do GCS. `Storage.upload(if_exists="replace")` age por blob, não apaga o prefixo — mas o header subiria como linha espúria, então o caminho limpo é alterar o schema da tabela externa pela API do BigQuery.

### 3. Modelo incremental não ganha coluna nova sem `--full-refresh`

`saldo` é `materialized="incremental"` e **não** define `on_schema_change`, então vale o default do dbt, que é `ignore`. Mesmo com a staging já corrigida, um `dbt run` comum roda verde e simplesmente não adiciona a coluna à tabela destino. Precisa de:

```bash
uv run dbt run --select br_bcb_sicor__saldo --full-refresh
```

Vale lembrar que o full-refresh reprocessa o histórico inteiro com `select distinct` mais o join do macro `add_ano_mes_operacao_data` — não é run barato. E o `pre_hook` do modelo dropa as row access policies, que voltam no `register_table_materialization` seguinte.

Referência de custo, do full-refresh feito em 30/07/2026 para o `indicador_renegociacao`: **641,2 milhões de linhas, 32,7 GiB processados, 58 segundos** de execução (2min52 no total, contando o parse do projeto).

Resultado esperado depois dele — a coluna só tem valor a partir do arquivo que a introduziu, e os anos anteriores ficam nulos, sem erro:

| ano | linhas | `indicador_renegociacao` preenchido |
|---|---|---|
| 2013 | 15.759.001 | 0 |
| 2025 | 53.136.509 | 1 |
| 2026 | 5.885.638 | 1.415.717 |

**Atenção: o full-refresh precisa ser feito em dev E em prod.** Nem o flow nem a action de table-approve passam `--full-refresh`, e ambos rodam `dbt run` comum — então mergear o código **não** leva a coluna à tabela de produção. Ou alguém com acesso roda o full-refresh em prod, ou o modelo passa a declarar `on_schema_change: append_new_columns`, que resolveria este caso e os próximos sem intervenção. A segunda opção muda comportamento e ainda não foi decidida.

### 4. O flow do `dicionario` não roda

`br_bcb_sicor__dicionario` é definido em `flows.py` **sem `deploy_schedules`**, então nunca executa por agendamento. Além disso passa `dbt_alias=False`, o que faz o seletor virar `models/br_bcb_sicor/dicionario.sql` — arquivo que não existe, já que o real é `br_bcb_sicor__dicionario.sql`. É o mesmo defeito do `br_rf_cafir` (issue #1700).

---

## Tabelas e Particularidades

### br_bcb_sicor__operacao

Tabela principal do conjunto de dados, contendo informações cadastrais básicas de todas as operações de crédito rural registradas no SICOR.

**Problemas Identificados:**
- Algumas colunas apresentam um percentual de valores nulos muito elevado, o que inviabiliza testes de `not_null` em múltiplas colunas simultâneas (ex: `data_inicio_plantio` e `data_inicio_colheita` com +80% de nulos) e diversas colunas possuem mais de 65% de nulos.

Esse comportamento é esperado. A base abriga registro de operações de crédito muito variadas e certas colunas não fazem sentido para certas operações. Por exemplo, para operações de pecuária não faz sentido ter um valor de data_inicio_plantio, por que não há plantio! Rs

**Log de Erro:**
```bash
Failure in test not_null_proportion_multiple_columns_br_bcb_sicor__operacao_0_65 (models/br_bcb_sicor/schema.yml)
12:48:22    Got 25 results, configured to fail if != 0
```

**Decisões e Tratamento:**
- As colunas `ano_emissao` e `mes_emissao` indicam a data de registro da operação no sistema e são fundamentais para o permissionamento.
- O teste de proporção de nulos (`not_null_proportion_multiple_columns`) foi desabilitado para evitar falhas falso-positivas devido à natureza dos dados originais.

---

### br_bcb_sicor__saldo

**Problemas Identificados:**
- Identificou-se cerca de 397 mil linhas com valores nulos para `ano_emissao` e `mes_emissao` após o join com a tabela de operações.
- Essas linhas estão associadas a aproximadamente 35 mil `id_referencia_bacen` que não constam nas tabelas de operação, liberação ou recursos públicos ("IDs fantasmas").

**Logs de Validação:**
```text
13:08:56  Coluna: mes_emissao - Resultado: FAIL - 'at_least' Recomendado: 0.99 - Quantidade Null: 397764 - Total: 639906093 - Proporção Null: 0.06
13:08:56  Coluna: ano_emissao - Resultado: FAIL - 'at_least' Recomendado: 0.99 - Quantidade Null: 397764 - Total: 639906093 - Proporção Null: 0.06
```

**Decisões e Tratamento:**
- **Remoção de nulos:** As linhas com IDs não encontrados na tabela de operações foram removidas da modelagem final.
- **Deduplicação:** Foi realizado um `distinct` para tratar duplicidades. Mesmo assim, 11 linhas (das 690M) apresentam valores de saldo divergentes para o mesmo par (ano, mes, id_referencia_bacen, numero_ordem), indicando erro na fonte.

**Queries de Debug:**
```sql
--- Verifica id_referencia_bacen que tem ano_emissao nulos após join com tabela operacao
select distinct
id_referencia_bacen
from basedosdados-dev.br_bcb_sicor.saldo
where ano_emissao is null;

--- Verifica se IDs fantasmas existem na tabela de liberação
with id_ref_bacen_mic_saldo as (
    select
    distinct id_referencia_bacen
    from basedosdados-dev.br_bcb_sicor.saldo
    where id_referencia_bacen not in (select distinct id_referencia_bacen from basedosdados-dev.br_bcb_sicor.operacao)
)
select id_referencia_bacen
from basedosdados-dev.br_bcb_sicor.liberacao
where id_referencia_bacen in (select id_referencia_bacen from id_ref_bacen_mic_saldo);

--- Identifica duplicidade de saldo por período e operação
with validation_errors as (
    select
        ano, mes, id_referencia_bacen, numero_ordem
    from `basedosdados-dev`.`br_bcb_sicor`.`saldo`
    group by ano, mes, id_referencia_bacen, numero_ordem
    having count(*) > 1
)
select * from validation_errors;
```

**Coluna `indicador_renegociacao` (nula em quase todo o histórico):**

O BCB só publica `IB_RENEGOCIADA` a partir do arquivo de base 2026-02. De fevereiro de 2026 em diante a coluna é 100% preenchida (`0`/`1`); janeiro de 2026 tem 49.688 preenchidas em 4,52 milhões de linhas; 2013 a 2025 é toda nula, com uma única linha de exceção em 2025. Não é artefato do `dump_mode="append"`: das 5,9 milhões de chaves de 2026 em dev, nenhuma tem uma versão nula e outra preenchida (verificado em 04/08/2026).

Duas consequências no `schema.yml`:

- a coluna entra em `ignore_values` do `not_null_proportion_multiple_columns`, que cobra `at_least: 1` das outras dez. Sem isso o teste é impossível e derruba o flow — foi o que aconteceu em 03/08/2026, quando o full-refresh materializou a coluna e o run que reaplicaria as row access policies falhou no teste;
- em troca, ela ganha um `not_null` próprio com escopo `(ano > 2026 or (ano = 2026 and mes >= 2))`.

O `where` tem duas escolhas que parecem redundantes e não são:

| Alternativa | Custo por execução | Em 2027 |
|---|---|---|
| `ano = 2026 and mes >= 2` | 98 MB | congela em 2026 — passa verde sem cobrir dado novo |
| `(ano > 2026 or (ano = 2026 and mes >= 2))` | 98 MB | acompanha os anos seguintes |
| `date(ano, mes, 1) >= '2026-02-01'` | 10,26 GB | acompanha, mas envolver `ano` numa função desliga o partition pruning |

Pendências de metadado, pelo manual de estilo: falta declarar a cobertura temporal da coluna (`2026-02(1)`) na planilha de arquitetura e no backend — deixar vazio significa "igual à tabela", o que é falso. Isso não é código: o `create_update_coverage` do MCP só aceita `table_id`, então vai por planilha ou GraphQL. O manual também define `indicador_` como booleano `int64` preenchido com 0/1, enquanto aqui a coluna é `STRING`, seguindo a regra do repo para flags 0/1 — divergência conhecida, cuja troca exigiria full-refresh em prod e mudança de `bigquery_type`.

---

### br_bcb_sicor__recurso_publico_mutuario

**Problemas Identificados:**
- **Ausência de dicionários:** As colunas `primeiro_mutuario` (valores 'N'/'S') e `sexo` (valores '1'/'2') não possuem dicionário oficial de tradução na fonte.
- **colunas cpf, cnpj_basico e cnpj:** A coluna `cnpj` possui 99,78% de valores nulos (consistente com operações de PF/Pronaf).

**Decisões e Tratamento:**
- **Manutenção de valores originais:** Valores mantidos conforme a fonte com descrições explicativas.
- **Criação de colunas:**

```sql
select
countif(length(tipo_cpf_cnpj) = 14) cnpj,
countif(length(tipo_cpf_cnpj) = 11) cpf,
countif(length(tipo_cpf_cnpj) = 8) cnpj_basico,
from `basedosdados-dev`.`br_bcb_sicor_staging`.`recurso_publico_mutuario`
```

- cnpj = 38.917
- cpf = 17697916
- cnpj_basico = 293


---

### br_bcb_sicor__recurso_publico_complemento_operacao

**Problemas Identificados:**
- Existência de 172 linhas de 22.968.008 com `id_municipio` nulo na fonte original.

---

### br_bcb_sicor__recurso_publico_cooperado

**Descrição:**
Informações sobre cooperados vinculados às operações.

**Problemas Identificados:**
- Mais do que um problema, é um ponto de atenção. O CNPJ informado é o CNPJ básico de 8 dígitos.

---

### br_bcb_sicor__recurso_publico_gleba

**Problemas Identificados:**
1. **Erros de WKT:** Falhas massivas na formatação Well-Known Text.
2. **Coordenadas 3D (Z):** Presença de altitude (ex: `-53.36 -32.18 0`).
3. **Ausência de Sinais Negativos:** Coordenadas brasileiras reportadas como positivas.

**Consulta para Identificar Geometrias Problemáticas:**
```sql
SELECT
    geometry as raw_string,
    safe_cast(id_referencia_bacen as string) as id_ref
FROM `basedosdados-dev.br_bcb_sicor_staging.recurso_publico_gleba`
WHERE SAFE.ST_GEOGFROMTEXT(geometry, make_valid=>TRUE) IS NULL;
```

**Decisões e Tratamento:**
Lógica de limpeza via SQL para remover dimensão Z e normalizar sinais de latitude/longitude.

**Query de Classificação e Validação de Sucesso:**
```sql
with
    raw_data as (
        select
            ano,
            geometria as geometria_original
        from
            basedosdados-dev.br_bcb_sicor_staging.recurso_publico_gleba
    ),
    cleaned_wkt as (
        select
            ano,
            geometria_original,
            regexp_replace(
                regexp_replace(
                    geometria_original,
                    r'([-+]?\d+\.?\d*)\s+([-+]?\d+\.?\d*)\s+[-+]?\d+\.?\d*',
                    r'\1 \2'
                ),
                r'(?i) Z ',
                ' '
            ) as stripped_wkt
        from raw_data
    ),
    normalized_wkt as (
        select
            *,
            regexp_replace(
                stripped_wkt, r'([ (\,])(\d+\.?\d*)', r'\1-\2'
            ) as fixed_negatives
        from cleaned_wkt
    ),
    geography_cast as (
        select
            *,
            safe.st_geogfromtext(fixed_negatives, make_valid => true) as geog_temp
        from normalized_wkt
    ),
    classification as (
        select
            ano,
            geometria_original,
            case
                when
                    geog_temp is not null
                    and not st_isempty(geog_temp)
                    and st_x(st_centroid(geog_temp)) between -74 and -34
                    and st_y(st_centroid(geog_temp)) between -34 and 6
                then 'Validated'
                when geometria_original is null
                then 'Null in Source'
                else 'Problematic'
            end as status
        from geography_cast
    )
select
    ano,
    countif(status = 'Validated') as qty_validated,
    countif(status = 'Problematic') as qty_problematic,
    countif(status = 'Null in Source') as qty_null_source,
    count(*) as total_rows,
    round(safe_divide(countif(status = 'Validated'), countif(status != 'Null in Source')) * 100, 2) as success_rate_pct
from classification
group by 1
order by 1 desc;
```

**Resultados da Validação:**

| Row | ano | qty_validated | qty_problematic | qty_null_source | total_rows | success_rate_pct |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 2026 | 79228 | 0 | 0 | 79228 | 100.0 |
| 2 | 2025 | 1008405 | 0 | 0 | 1008405 | 100.0 |
| 3 | 2024 | 1167995 | 0 | 0 | 1167995 | 100.0 |
| 4 | 2023 | 1109957 | 0 | 0 | 1109957 | 100.0 |
| 5 | 2022 | 1062339 | 0 | 0 | 1062339 | 100.0 |
| 6 | 2021 | 980803 | 0 | 0 | 980803 | 100.0 |
| 7 | 2020 | 888805 | 0 | 0 | 888805 | 100.0 |
| 8 | 2019 | 608728 | 0 | 0 | 608728 | 100.0 |
| 9 | 2018 | 395235 | 0 | 0 | 395235 | 100.0 |
| 10 | 2017 | 313718 | 26 | 0 | 313744 | 99.99 |
| 11 | 2016 | 91071 | 28 | 0 | 91099 | 99.97 |
| 12 | 2015 | 8914 | 35 | 0 | 8949 | 99.61 |
| 13 | 2014 | 1880 | 91 | 0 | 1971 | 95.38 |
| 14 | 2013 | 501 | 146 | 0 | 647 | 77.43 |

---

### br_bcb_sicor__recurso_publico_propriedade



**Problemas Identificados:**
-  O CAR só existe consistentemente a partir de 2018; apenas em 2018 o banco central passou a cobrar o preenchimento do CAR como exigência para a concessão do empréstimo;
- A coluna `id_cib` (até 29/07/2026 chamada `id_nirf` na fonte) é **inteiramente vazia**: zero valores preenchidos em 27 milhões de linhas, de 2013 a 2026. A fonte sempre entregou `-1`. Vale saber antes de tentar usá-la em qualquer análise — e ao decidir se a coluna deve continuar publicada.

---

### br_bcb_sicor__liberacao

**Problemas Identificados:**
- **Anomalias de Data:** Datas de liberação em anos impossíveis (1905, 2011) ou futuros (2028).

**Query de Verificação de Anomalias:**
```sql
select
    EXTRACT(YEAR FROM PARSE_DATE("%d/%m/%Y", data_liberacao)) AS ano_liberacao,
    count(*)
from basedosdados-dev.br_bcb_sicor_staging.liberacao
group by all
order by ano_liberacao;
```

**Decisões e Tratamento:**
- Linhas com anos inconsistentes com a existência do sistema (anteriores a 2013) ou futuros foram removidas.

---
### br_bcb_sicor__operacoes_desclassificadas

**Problemas Identificados:**
- O único problema é a existência de valor da coluna id_motivo_desclassificacao que não existem no dicionário oficial do sicor
São eles: ["0", "201", "14"]

---

## O código zero, `ltrim` e a cobertura do dicionário

Esta é a armadilha mais fácil de disparar sem perceber neste conjunto, e ela
atravessa quase todas as tabelas.

A fonte publica os códigos com zeros à esquerda: `TipoCultivo` traz `"00"`,
`FonteRecursos` traz `"0100"`. Os modelos aplicam `ltrim(coluna, '0')` para
normalizar, e o modelo `dicionario` aplica `ltrim(chave, '0')` pela mesma razão.
Nos dois lados, portanto, um código composto **só de zeros** — `"0"`, `"00"` —
vira **string vazia**. E é exatamente assim que o código "Não se aplica" casa
hoje: `''` de um lado, `''` do outro.

Oito das tabelas de domínio definem esse código zero (`TipoCultivo`,
`GraoSemente`, `FaseCicloProducao`, `TipoIrrigacao`, `TipoIntegracao`,
`EncargosFinanceirosComplementares`, `TipoSoloProagro`,
`TipoGarantiaEmpreendimento`), e em `operacao` ele não é raro: `id_tipo_cultivo`
tem 24.936.161 linhas com string vazia em 29.675.448 — o valor bruto é `"00"`.

**Consertar um lado só quebra o outro.** Trocar o `ltrim` do `dicionario` por
algo que preserve o zero (`coalesce(nullif(ltrim(chave, '0'), ''), '0')`) faz o
dicionário passar a dizer `'0'` enquanto `operacao` continua dizendo `''`, e a
cobertura de oito colunas cai de uma vez. O conserto correto é bilateral, nos
dois modelos ao mesmo tempo, e em `operacao` ele custa um `--full-refresh` em dev
**e** em prod (29,7 milhões de linhas), porque o modelo é incremental com
`on_schema_change` no default. Não foi feito aqui; fica registrado como decisão
pendente.

Nas tabelas novas do Proagro o `ltrim` foi mantido, por consistência: a única
coluna afetada é `proagro_cop.id_tipo_solo`, e o código 0 aparece em **19**
linhas de 1.004.175.

### `id_tipo_agricultura` é a exceção

`id_tipo_agricultura` é a única das quinze colunas de código de `operacao` cujo
SELECT **não** aplica `ltrim`. Ela publica `'0'` em 21.978.645 linhas, enquanto o
dicionário reduz o mesmo código a `''`. Por isso ela está fora da lista
`columns_covered_by_dictionary` de `operacao`, com a exceção registrada na
descrição do modelo. Incluí-la exige o mesmo `--full-refresh` citado acima.

---

## Quatro testes de dicionário que nunca rodaram

Até este PR, quatro testes `custom_dictionary_coverage` de `br_bcb_sicor`
apontavam para modelos inexistentes:

| modelo | ref errado | erro |
|---|---|---|
| `operacao` | `br_bcb_sicar__dicionario` | "sicar" no lugar de "sicor" |
| `empreendimento` | `br_bcb_sicor_dicionario` | um underscore no lugar de dois |
| `recurso_publico_mutuario` | `br_bcb_sicor_dicionario` | idem |
| `recurso_publico_cooperado` | `br_bcb_sicor_dicionario` | idem |

**O dbt não falha nesse caso — ele emite `WARNING` e descarta o teste.** O
resultado é um teste que aparece no `schema.yml`, não roda nunca e nada acusa:

```text
[WARNING]: Test 'custom_dictionary_coverage_br_bcb_sicor__operacao_...' 
depends on a node named 'br_bcb_sicar__dicionario' in package '' which was not found
```

Eram 18 colunas sem cobertura nenhuma. Os quatro agora apontam para
`br_bcb_sicor__dicionario` e passam.

Vale como lição geral: um `ref()` dentro de argumento de teste é silencioso
quando quebra. Ao renomear um modelo, conferir os `ref()` dos testes, não só os
dos modelos.

---

## O flow do `dicionario` (corrigido)

A seção "4. O flow do `dicionario` não roda", acima, descrevia dois defeitos.
Ambos foram corrigidos neste PR, porque eles bloqueavam a cobertura de
dicionário das tabelas novas:

- `dbt_alias=False` fazia o seletor virar `models/br_bcb_sicor/dicionario.sql`,
  arquivo que não existe — `run_dbt` levantava `FileNotFoundError` **depois** de
  já ter subido o staging. Agora usa o default `dbt_alias=True`.
- Faltava `deploy_schedules`, então o flow nunca rodava por agendamento. Agora
  roda às 01:45 nos dias úteis, **antes** de `operacao` (02:05) e das demais, já
  que o teste de cada tabela lê este modelo.

Consequência prática de a tabela estar congelada: o código `0911` (FNDCT, MP
1374) já existia em `FonteRecursos.csv` e não no `dicionario` materializado, o
que derrubaria a cobertura de `operacao.id_fonte_recurso` no instante em que o
teste voltasse a rodar.

No mesmo movimento, `operacao.id_tipo_seguro` era declarado como coberto por
dicionário sem que a fonte (`TipoGarantiaEmpreendimento.csv`) estivesse
registrada em `Constants.dicionario`. Registrada.

---

## `valor_percentual_*`: nome divergente em `operacao`

Três colunas de `operacao` existem no BigQuery como
`valor_percentual_{custo_efetivo_total,risco_fundo_constitucional,risco_stn}`,
mas o `schema.yml` e o backend as chamavam `percentual_*`. O `schema.yml` foi
alinhado ao que está materializado, senão o `persist_docs` do dbt não escreve
descrição em coluna nenhuma e o check `Metadata validation (BigQuery vs API)`
acusa divergência.

O nome correto pelo manual de estilo é `percentual_*` — `valor_` e `percentual_`
são prefixos alternativos, não acumuláveis, e a coluna irmã
`percentual_bonus_car` está na forma certa. Renomear de verdade exige mexer no
`.sql` e um `--full-refresh` em dev e prod. Fica registrado como pendência de
estilo, não corrigido aqui.

Aproveitando: as três colunas de CNPJ de `operacao`
(`cnpj_basico_instituicao_financeira`, `cnpj_basico_agente_investimento`,
`cnpj_basico_cadastrante`) existem no BigQuery e **não** existiam nos metadados
de prod. Como o check de metadados só roda sobre arquivos `.sql` modificados, e
`operacao.sql` não era tocado havia tempo, a divergência nunca apareceu.

---

## Tabelas de domínio: `instituicao_financeira` e `fonte_recurso`

### `br_bcb_sicor__instituicao_financeira`

Diretório das 650 instituições financeiras do Sicor (`IFsSicor` no manual,
publicado como `DadosBrutos/SICOR_LISTA_IFS.csv`). É a tabela que faltava para
separar crédito rural por credor.

**`operacao` sempre teve a coluna do credor** —
`cnpj_basico_instituicao_financeira`, de `CNPJ_IF`. O que faltava era o nome e o
segmento. Medido em dev:

| verificação | resultado |
|---|---|
| linhas de `operacao` | 29.675.448 |
| linhas cujo CNPJ casa com `LISTA_IFS` | **29.675.448 (100%)** |
| CNPJs distintos em `operacao` / em `LISTA_IFS` | 628 / 650 |
| linhas com CNPJ nulo ou vazio | 0 |

Ou seja, dá para quebrar o crédito rural por credor desde 2013 sem nenhuma
perda, e sem depender de `recurso_publico_complemento_operacao.cnpj_agencia`,
que identifica a agência e só existe para operações de fonte pública.

Segmentos: 566 cooperativas de crédito, 45 bancos privados, 10 bancos de
desenvolvimento e agências de fomento, 8 bancos públicos, 4 sociedades de
crédito, 2 bancos cooperativos e **15 linhas com segmento em branco na fonte**.
Nome e segmento ficam em caixa alta como publicados; `initcap` estragaria siglas
("BCO DO BRASIL S.A.", "CC ARACREDI LTDA.").

### `br_bcb_sicor__fonte_recurso`

Junta duas tabelas de domínio. `FonteRecursos.csv` tem os 37 códigos com
descrição e vigência; `FonteRecursosPublicos.csv` tem 16 deles, **com descrições
idênticas**. Verificado: os 16 são subconjunto estrito dos 37, e a única
diferença de texto é um `\x96` perdido no código 0911 do arquivo latin-1.

Como o segundo arquivo não traz descrição nova, seu conteúdo é apenas a
participação na lista — publicada como `indicador_recurso_publico`. Virar linhas
de `dicionario` não funcionaria: colidiria com as chaves de `id_fonte_recurso`
que já vêm do primeiro arquivo.

**E é essa lista que define o universo das tabelas `recurso_publico_*`.** As 16
fontes públicas incluem 0100 Tesouro Nacional, 0300 poupança rural controlada,
0501 FNO, 0502 FNE, 0503 FCO, 0505 BNDES/Finame, 0650 FAT, 0800 Funcafé. Ficam
de fora, entre outras, 0201 obrigatórios MCR 6.2, 0402 recursos livres,
0430/0440 LCA a taxa livre e favorecida, e 0303 poupança rural não controlada —
exatamente as fontes com cobertura abaixo de 2% em `recurso_publico_propriedade`.

Dois detalhes de formato, porque os dois arquivos divergem: o de todas as fontes
é **latin-1 com `;`**, o de fontes públicas é **UTF-8 com `,`**.

---

## O Proagro

O Proagro (Programa de Garantia da Atividade Agropecuária) cobre perdas do
produtor em lavoura amparada. O conjunto se chama "Crédito Rural **e do
Proagro**" e não tinha nenhuma tabela do Proagro até este PR.

O fluxo tem quatro etapas, e cada uma é uma tabela:

```
COP          o produtor comunica a perda            proagro_cop
 │                                                  + proagro_complemento_cop (periciadora/perito)
 ▼
RCP          requerimento de cobertura, com         proagro_rcp
 │           o laudo da vistoria                    + proagro_complemento_rcp (periciadora/perito)
 │                                                  + proagro_rcp_gleba (geometria vistoriada)
 ▼
Julgamento   decisão e memória de cálculo           proagro_sumula_julgamento
 │
 ▼
Pagamento    lançamentos financeiros                proagro_parcela
```

Todas se ligam a `operacao` por `id_referencia_bacen` + `numero_ordem`, e
herdam daí `ano_emissao`/`mes_emissao`, o particionamento e o permissionamento
BD PRO.

| tabela | linhas | cobertura | sem par em `operacao` |
|---|---|---|---|
| `proagro_cop` | 1.004.175 | 2013–2026 | 61 (0,006%) |
| `proagro_complemento_cop` | 1.004.175 | 2013–2026 | 61 (0,006%) |
| `proagro_rcp` | 846.510 | 2014–2026 | 62 (0,007%) |
| `proagro_complemento_rcp` | 846.510 | 2014–2026 | 62 (0,007%) |
| `proagro_rcp_gleba` | 1.760.631 | 2015–2026 | 130 (0,007%) |
| `proagro_parcela` | 13.381.107 | 2013–2026 | 983 (0,007%) |
| `proagro_sumula_julgamento` | 837.433 | 2016–2026 | 59 (0,007%) |

As linhas sem par são os mesmos "IDs fantasmas" já descritos na seção de
`saldo`. Note que **cada tabela tem sua própria cobertura inicial** — 2013,
2014, 2015 e 2016 —, o que importa ao registrar o `DateTimeRange` de cada uma.

Os arquivos `COMPLEMENTO_*` e `RCP_GLEBAS` estão na seção 3 do site, a das
tabelas complementares dos recursos públicos, mas **não** têm a restrição de
universo que as tabelas `recurso_publico_*` têm: cada um tem exatamente o mesmo
número de linhas do seu par da seção 2, e o manual marca a relação como 1..1.
Foram mantidos como tabelas separadas, uma por arquivo da fonte, seguindo o
padrão de `recurso_publico_complemento_operacao`.

### Datas: dd/mm/yyyy, e anos transpostos

**As 20 colunas de data das tabelas do Proagro são dd/mm/yyyy**, verificado
valor por valor nos arquivos completos: 630.453 valores de `data_comunicacao`
têm primeiro componente > 12 e **nenhum** tem segundo componente > 12. Não é o
formato de `operacoes_desclassificadas`, que usa `%m/%d/%Y %H:%M:%S`.

As colunas **administrativas** (`data_comunicacao`, `data_entrega`,
`data_visita`, e todas as de `proagro_parcela` e da súmula) estão limpas: zero
valores fora de 2013–2027.

As colunas **agronômicas** não. Elas trazem erros de digitação com o ano
transposto, que o BigQuery aceita como datas perfeitamente válidas:

| valor publicado | ano provável |
|---|---|
| `02/10/3202` | 2023 |
| `06/11/1202` | 2021 |
| `20/08/0217` | 2017 |
| `25/01/5202` | 2025 |

Os extremos: `data_fim_colheita` chega a **2224-01-18** em `proagro_cop` e
**2241-05-01** em `proagro_rcp`; `data_inicio_plantio` desce a **1914-07-02**.

O macro `parse_data_agronomica_sicor` anula o que cai fora de **2000–2100**. A
janela foi escolhida medindo: ela atinge no máximo 0,072% de qualquer coluna
afetada, enquanto 2010–2030 atingiria 0,57% e passaria a anular valores
possivelmente legítimos.

| coluna | fora de 2000–2100 | fora de 2010–2030 |
|---|---|---|
| `proagro_cop.data_inicio_plantio` | 720 (0,0717%) | 5.744 (0,5720%) |
| `proagro_cop.data_fim_plantio` | 619 (0,0616%) | 5.206 (0,5184%) |
| `proagro_rcp.data_inicio_plantio` | 281 (0,0332%) | 2.606 (0,3079%) |
| `proagro_rcp.data_fim_plantio` | 241 (0,0285%) | 2.355 (0,2782%) |

É a mesma ideia do tratamento em `operacao`, que ali é unilateral (`> 2100`);
aqui a janela é bilateral porque os erros aparecem nas duas pontas. Os testes de
`relationships` contra o diretório de datas ficam só nas colunas
administrativas.

**Todo o parsing de data das tabelas do Proagro usa `safe.parse_date`**, tanto
no macro quanto nas três colunas administrativas que não passam por ele
(`data_comunicacao`, `data_entrega`, `data_visita`). O `parse_date` cru levanta
erro e aborta o modelo inteiro por uma única célula ilegível — o que numa fonte
republicada mensalmente é pior que anular o valor. Hoje a troca não muda
nenhuma linha: medido sobre os arquivos completos, as nove colunas de data
dessas duas tabelas têm **zero** valores que o BigQuery não consegue ler como
`%d/%m/%Y` (1.004.175 linhas em `proagro_cop`, 846.510 em `proagro_rcp`), e as
três administrativas também têm zero fora de 2000–2100 — os anos vão de 2013 a
2026. O filtro de janela continua sendo necessário só nas agronômicas, porque
lá o defeito produz datas *válidas* e absurdas, que nenhum `safe.` pega.

### `proagro_cop`: a chave primária do manual não é única

O manual declara (REF_BACEN, NU_ORDEM, CD_EVENTO) como chave primária do
`sicor_cop_basico`. Ela não é:

- 4.225 linhas (0,42%) repetem a chave;
- **4.120 delas trazem valores diferentes** de `data_comunicacao`, `id_status` ou
  `id_tipo_ciclo_cultivar` — são comunicados distintos para o mesmo evento na
  mesma operação;
- 105 são idênticas em todas as 11 colunas.

Exemplo real (ref_bacen 10002320, ordem 2, evento 17):

```text
data_comunicacao  id_status  id_tipo_ciclo_cultivar
22/10/2017        2          99
23/10/2017        5           1
```

Nenhuma combinação de colunas publicadas torna a tabela única: acrescentando
`data_comunicacao` sobram 1.762 duplicatas; com `id_status` também, 143; com o
ciclo, 141. **Não se aplica deduplicação** — um `select distinct` destruiria
registros reais. O teste usa `custom_unique_combinations_of_columns` e a exceção
está na descrição do modelo.

O mesmo vale para `proagro_complemento_cop`, onde 2.806 linhas são idênticas em
todas as colunas e não podem ser separadas por chave alguma.

As outras cinco tabelas têm chave única, verificado na tabela inteira e depois
dos casts (`proagro_parcela` e `proagro_rcp_gleba`: 0 chaves duplicadas em
13.381.107 e 1.760.631 linhas).

### `proagro_complemento_cop`: CPF/CNPJ sem os zeros à esquerda

`CD_CPF_CNPJ_PERICIADORA` deveria ter 8, 11 ou 14 dígitos. No arquivo do COP tem
de 5 a 14:

| comprimento | linhas | leitura |
|---|---|---|
| 14 | 658.082 | CNPJ completo |
| 11 | 202.260 | CPF |
| 8 | 109.335 | CNPJ básico |
| 5, 6, 7, 9, 10, 12 | 34.498 | zeros à esquerda perdidos |

A separação por comprimento — a mesma de `recurso_publico_mutuario` — resolve
969.677 linhas (96,6%) e deixa **34.498 (3,44%)** nulas nas três colunas de
identificação. Não é possível recuperá-las: um valor de 10 dígitos pode ser um
CPF ou um CNPJ básico truncado, sem como decidir.

**O arquivo irmão do RCP não tem o problema**: só comprimentos 11 (481.124) e 14
(365.386), separação completa. Por isso `proagro_complemento_rcp` não tem coluna
de CNPJ básico — ela seria inteiramente nula.

`CD_CPF_PERITO` tem sempre 11 dígitos quando preenchido, em 25,6% das linhas do
COP e 30,7% das do RCP.

### `proagro_rcp_gleba`: dois arquivos disjuntos

A fonte divulga as glebas do RCP em dois arquivos plurianuais em vez de um por
ano: `SICOR_RCP_GLEBAS_2015_2020` (420.949 linhas) e `SICOR_RCP_GLEBAS_2021`
(1.339.682). **A interseção das chaves entre eles é zero**, então a união é
direta e não duplica nada. Por não haver quebra anual, o modelo é
`materialized="table"` e não incremental, ao contrário de
`recurso_publico_gleba`.

A limpeza de WKT é a mesma de `recurso_publico_gleba` (remove a dimensão Z,
corrige sinais para o hemisfério Sul/Oeste, aplica `make_valid`, anula centroides
fora do bounding box do Brasil). Aqui a fonte é muito melhor: **4** das 1.760.631
glebas não sobrevivem ao tratamento, contra 22,6% em 2013 na tabela de operações.

### Códigos sem dicionário

Três colunas das tabelas novas não têm tradução em nenhuma das 57 tabelas de
domínio da seção 1:

| coluna | valores observados | observação |
|---|---|---|
| `proagro_sumula_julgamento.id_decisao` | 2, 3, 4, 5, 6 | o manual descreve `CD_DECISAO` erradamente como "Data da decisão na súmula de julgamento" |
| `proagro_sumula_julgamento.id_status` | 1 | valor único em todas as linhas |
| `proagro_rcp.id_status` | 1 | valor único em todas as linhas |
| `proagro_rcp.id_tipo` | 1, 2 | — |

Todas as demais colunas de código estão cobertas, e **nenhum valor ficou órfão** —
não foi preciso recorrer ao macro `dicionario_not_found`.

### Divergências entre o manual e os arquivos

- O diagrama do manual desenha `sic_REF_BACEN` e `CD_EVENTO` em
  `sicor_complemento_rcp`; o arquivo publicado tem só quatro colunas
  (`REF_BACEN`, `NU_ORDEM`, `CD_CPF_CNPJ_PERICIADORA`, `CD_CPF_PERITO`).
- O manual marca `REF_BACEN` como "mascarado" em algumas tabelas e não em outras
  (`sicor_cop_basico`, `sicor_liberacao_recursos` e `sicor_desclassificacao` sem
  a palavra; `sicor_operacao_basica_estado`, `sicor_saldos` e o resto com). A
  distinção não existe na prática — todas casam entre si.
- A chave primária do `sicor_cop_basico`, como descrito acima.
- A descrição de `CD_DECISAO`.

### Os arquivos `.hash` não cobrem estas tabelas

O site publica 45 arquivos `.hash`, e eles cobrem **apenas**
`SICOR_OPERACAO_BASICA_ESTADO_*`, `SICOR_SALDOS_*`, `sicor_glebas_wkt_*` e
`SICOR_GLEBAS_CONTRAT`. Nenhum dos arquivos do Proagro, nem
`SICOR_LISTA_IFS.csv`, tem um irmão `.hash` — todos retornam 404. A verificação
de download destas tabelas foi feita comparando o `Content-Length` publicado com
o tamanho baixado, e depois a contagem de linhas do arquivo com a contagem de
linhas do parquet gerado.

---

## O que ainda não está na Base dos Dados

Depois deste PR, o `br_bcb_sicor` tem 20 tabelas. Segue o que resta dos cinco
blocos do site do BCB, e por quê.

### Seção 2 — Sicor, crédito rural e Proagro

| arquivo | situação |
|---|---|
| `SICOR_PARCELAS_DESEMBOLSO.gz` | **não onboardado.** 1,06 GB comprimido, o maior arquivo do conjunto. Cronograma de desembolso; interessa a quem mede o intervalo entre contratação e liberação efetiva |
| `SICOR_LISTA_RENEGOCIACAO.gz` | **não onboardado** |
| `SICOR_OPERACAO_BASICA_RENEGOCIACAO.gz` | **não onboardado.** Mesma estrutura de `sicor_operacao_basica_estado`, segundo o manual |
| `SICOR_LISTA_ALTERACAO_FONTE.gz` | **não onboardado** |

### Seção 3 — complementares dos recursos públicos

| arquivo | situação |
|---|---|
| `SICOR_COMPLEMENTO_OPERACAO_BASICA_RENEGOCIACAO.gz` | **não onboardado.** Par do de renegociação acima |

### Seção 4 — Recor/PGRO, 1983 a 2012

Nada onboardado: 30 arquivos anuais de operações, mais mutuários, municípios,
COP, parcelas, renegociações e 15 tabelas de domínio próprias. **Decisão ainda
não tomada** sobre conjunto separado (`br_bcb_recor`) ou extensão deste.

Um ponto a registrar desde já, porque os usuários vão supor o contrário: o Recor
é inteiramente **pré-CAR**. Ele tem mutuário e município, mas nenhuma chave de
registro de imóvel, então serve para estender uma série municipal de crédito
rural até 1983 e **não** serve para estender uma série no nível do imóvel.

### Seção 5 — Sicor contratado

Nada onboardado (6 arquivos). **Antes de modelar, é preciso estabelecer o que
"Contratado" significa em relação às tabelas principais** — universo disjunto,
superconjunto, ou a mesma coisa em outro momento. O manual do Sicor **não
menciona nenhuma das tabelas `*_CONTRAT`** na sua lista de tabelas, então a
resposta terá de vir da comparação empírica de `id_referencia_bacen` contra
`br_bcb_sicor__operacao`.

### Seção 1 — tabelas de domínio

Das 57 tabelas de domínio, o `dicionario` agora cobre as colunas de código
efetivamente usadas nas 20 tabelas do conjunto, e três viraram tabelas próprias:
`empreendimento`, já existente, e `instituicao_financeira` e `fonte_recurso`,
adicionadas neste PR. As
restantes descrevem colunas que o conjunto não publica (prazos por programa, por
fonte e por UF, tipos de clima, manejo, conformidade, bônus, motivos de exclusão
e de rejeição de saldo, municípios do Sicor) ou duplicam diretórios da Base dos
Dados.

---

## Cobertura das tabelas `recurso_publico_*`: não é dado faltante

O ponto que os usuários mais erram neste conjunto. As cinco tabelas
`recurso_publico_*` cobrem **apenas operações financiadas com fontes
públicas/controladas** — exatamente as 16 fontes que `fonte_recurso` marca com
`indicador_recurso_publico = 1`. Uma operação com fonte livre simplesmente não
aparece nelas.

Cobertura em `recurso_publico_propriedade` por classe de fonte: ~99% para FNO,
FCO, poupança rural controlada, BNDES/Finame e demais subsidiadas; **abaixo de
2%** para recursos livres, LCA a taxa livre, poupança rural livre e obrigatórios
MCR 6.2. É o desenho da fonte, não defeito. A ressalva foi adicionada às
descrições das cinco tabelas.

### `id_car`: formato, cobertura e defeitos

Medido sobre as 27.796.342 linhas de `recurso_publico_propriedade` em dev:

| | |
|---|---|
| linhas com CAR | 15.463.388 (**55,6%**) |
| comprimento | **41 caracteres em 100% dos casos**, nenhum hífen |
| UF inválida nos 2 primeiros caracteres | 266 linhas (`AA`, `AB`, `MH`, `UF`) |

O Sicor publica o CAR **não hifenizado, 41 caracteres** (UF + 7 dígitos do
município IBGE + 32 hexadecimais), enquanto o registro do SFB em
`basedosdados.br_sfb_sicar.area_imovel` usa a forma **hifenizada de 43
caracteres**. Quem cruzar as duas bases tem de normalizar antes — não há join
direto. As 266 linhas com UF inválida são defeitos da fonte e ficam como
publicadas, sem descarte.

O preenchimento começa em 2018, quando o Banco Central passou a exigir o CAR
para a concessão do crédito: 0% de 2013 a 2016, cerca de 12% em 2018 e 55–59% de
2019 em diante. Registrado na descrição da coluna.

---

## Registro de metadados: staging, e o que falta em prod

Os metadados das nove tabelas novas foram registrados no backend de **staging**,
não no de dev — o de dev (`development.backend.basedosdados.org`) esteve
retornando **503** de forma persistente em 30/09/2026, enquanto staging e prod
respondiam normalmente.

**Saiba que o registro de `sicor` em staging é um retrato antigo e divergente de
prod.** O que está lá e não deveria:

| | staging | prod |
|---|---|---|
| nomes de tabela | `microdados_operacao`, `microdados_saldo`, `microdados_liberacao` | `operacao`, `saldo`, `liberacao` |
| `operacoes_desclassificadas` | ausente | presente |
| colunas de `operacao` | `plano_safra_emissao`, sem `ano_emissao`/`mes_emissao` | `ano_safra_emissao`, com as duas |
| `recurso_publico_propriedade` | `id_nirf` | `id_cib` |
| `recurso_publico_gleba` | `altitude`, `ponto`, `indice_ponto` | `geometria`, `geometria_original` |
| slugs de tag | em português (`agropecuaria`, `credito`) | em inglês (`agriculture`, `credit`) |

Nada disso foi tocado — só as nove tabelas novas foram criadas. **Cuidado ao
promover staging → prod**: isso reverteria os nomes das três tabelas acima para a
forma `microdados_*`, que já foi renomeada em prod.

### Detalhes do registro em staging

- `gcp_project_id = basedosdados-dev` nas cloud tables, porque é onde os dados
  estão hoje. **Na promoção para prod, tem de ser `basedosdados`.**
- As sete tabelas do Proagro têm as duas Coverages que um pipeline `part_bdpro`
  exige, com faixas não sobrepostas e `is_closed` na Coverage **e** no
  DateTimeRange: livre até 2026-02, BD Pro de 2026-03 a 2026-08 (`free_lag` de 6
  meses sobre o máximo de 2026-08). Sem as duas, `assert_coverage_topology`
  derruba o primeiro run.
- `instituicao_financeira` e `fonte_recurso` são `all_free` e têm uma Coverage só.
- Cada tabela tem exatamente uma cloud table, um observation level, um Update e
  **uma** raw data source — o limite de uma por tabela é necessário, porque
  `client._raw_source_id` levanta erro com duas ou mais e o poll do pipeline
  passa por ele.

### Duas armadilhas encontradas na API de colunas

- **A chave do `columns_json` é `directory_column`, não `directory_column_name`.**
  A segunda é o *parâmetro* do `update_column` e não vale aqui: passá-la é um
  no-op silencioso, e foi o que deixou as nove tabelas sem um único vínculo de
  diretório enquanto a chamada reportava sucesso. `is_partition` realmente não
  existia no caminho em lote e exigia um `update_column` por coluna.
  **Os dois foram corrigidos** no repo `mcp`, branch
  `fix/directory-column-silent-failures`: chave desconhecida e falha de lookup
  agora aparecem em `errors`, e `is_partition`/`is_primary_key` passaram a ser
  aceitos como chaves opt-in.
- **`directoryPrimaryKey` só aceita coluna marcada como chave primária de uma
  tabela de diretório** (`limit_choices_to` no modelo). Em
  `br_bd_diretorios_brasil.empresa` apenas `cnpj` está marcada, não
  `cnpj_basico` — então `instituicao_financeira.cnpj_basico` **não pode** ser
  vinculada. **Isso está correto, não é defeito**: a tabela tem 72.789.638 linhas
  com `cnpj` único e só 69.523.303 `cnpj_basico` distintos, então a FK seria
  ambígua. A integridade fica garantida pelo teste dbt `relationships`, que
  passa — é o mesmo motivo pelo qual nenhuma coluna de CNPJ de `operacao` tem
  vínculo de diretório em prod. O `mcp` agora devolve essa explicação em vez de
  deixar passar o `Faça uma escolha válida` do Django.
