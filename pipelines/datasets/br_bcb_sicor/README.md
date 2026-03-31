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
