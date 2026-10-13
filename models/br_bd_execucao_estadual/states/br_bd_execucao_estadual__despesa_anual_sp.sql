{{ config(materialized="ephemeral") }}

-- São Paulo annual budget execution, mapped onto the canonical `despesa_anual` schema.
--
-- Source: SIGEO Lei 131, scraped from the WebForms consultation one (exercise, órgão)
-- at
-- a time. The export has no document number and no sub-annual date at all -- SIGEO
-- aggregates to the year -- which is why SP cannot enter `despesa` and has its own
-- table.
-- What it does carry, and MG's and PE's exports do not carry together, is the creditor
-- against a fully specified budget line.
--
-- Every dimension arrives as "<code> - <name>" in a single field. The split takes the
-- FIRST " - " only: 13.1% of creditor names and 12.0% of unidade gestora names contain
-- another " - " ("RECURSOS NAO VINC DE IMPOSTOS - TESOURO"), so a greedy split would
-- cut
-- the name in half. Measured across 3.97M scraped rows the split succeeds on 99.99% of
-- them; the remainder keep the raw string as the name and a null code, rather than
-- being
-- dropped.
with
    parsed as (
        select
            ano,
            regexp_extract(orgao, r'^(.*?) - ') as cd_orgao,
            regexp_extract(orgao, r'^.*? - (.*)$') as nm_orgao,
            regexp_extract(unidade_gestora, r'^(.*?) - ') as cd_ug,
            regexp_extract(unidade_gestora, r'^.*? - (.*)$') as nm_ug,
            regexp_extract(fonte_recurso, r'^(.*?) - ') as cd_fonte,
            regexp_extract(fonte_recurso, r'^.*? - (.*)$') as nm_fonte,
            regexp_extract(credor, r'^(.*?) - ') as cd_credor,
            regexp_extract(credor, r'^.*? - (.*)$') as nm_credor,
            regexp_extract(despesa, r'^(.*?) - ') as cd_despesa,
            regexp_extract(despesa, r'^.*? - (.*)$') as nm_despesa,
            orgao as raw_orgao,
            unidade_gestora as raw_ug,
            fonte_recurso as raw_fonte,
            credor as raw_credor,
            despesa as raw_despesa,
            empenhado,
            liquidado,
            pago,
            pago_restos
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sp_despesa") }}
        -- EVERY export ends with a TOTALS row: all five dimension fields empty, the
        -- Despesa field literally "TOTAL", and values equal to the sum of the rows
        -- above
        -- it. One per file, 509 of them, carrying R$12.7 trillion -- almost exactly
        -- half
        -- the table. Left in, every aggregate is exactly double.
        --
        -- It also survives a raw-vs-model reconciliation, because the totals sit on
        -- both
        -- sides: the sums match perfectly and are both wrong. What exposes it is that
        -- the
        -- rows have no órgão, which is how they are dropped here.
        where trim(coalesce(orgao, '')) != ''
    )
select
    safe_cast(ano as int64) as ano,
    'SP' as sigla_uf,
    safe_cast(nullif(trim(cd_orgao), '') as string) as orgao,
    safe_cast(nullif(trim(coalesce(nm_orgao, raw_orgao)), '') as string) as nome_orgao,
    safe_cast(nullif(trim(cd_ug), '') as string) as id_unidade_gestora,
    safe_cast(
        nullif(trim(coalesce(nm_ug, raw_ug)), '') as string
    ) as nome_unidade_gestora,
    safe_cast(nullif(trim(cd_fonte), '') as string) as fonte_recurso,
    safe_cast(
        nullif(trim(coalesce(nm_fonte, raw_fonte)), '') as string
    ) as nome_fonte_recurso,
    -- SIGEO's "Despesa" is the full natureza da despesa -- eight digits carrying
    -- category, group, application modality, element and subelement at once -- not the
    -- `elemento_despesa` that `despesa` publishes for MG and PE. It is named for what
    -- it
    -- is rather than forced into the narrower column.
    safe_cast(nullif(trim(cd_despesa), '') as string) as natureza_despesa,
    safe_cast(
        nullif(trim(coalesce(nm_despesa, raw_despesa)), '') as string
    ) as nome_natureza_despesa,
    -- São Paulo publishes creditor CPFs UNMASKED, unlike Minas Gerais, which redacts
    -- them
    -- as INFORMACAO COM RESTRICAO DE ACESSO, and Pernambuco, which masks them as
    -- ***.721.874-**. 29.8% of rows carry an eleven-digit code, of which 97% satisfy
    -- the
    -- CPF check digits, and they are paired with the individual's full name.
    -- Published as
    -- the source publishes it, flagged has_sensitive_data.
    safe_cast(nullif(trim(cd_credor), '') as string) as documento_credor,
    safe_cast(
        nullif(trim(coalesce(nm_credor, raw_credor)), '') as string
    ) as nome_credor,
    case
        when regexp_contains(trim(cd_credor), r'^\d{14}$')
        then 'CNPJ'
        when regexp_contains(trim(cd_credor), r'^\d{11}$')
        then 'CPF'
        when regexp_contains(trim(cd_credor), r'^\d{6}$')
        then 'UNIDADE_GESTORA'
        when trim(coalesce(cd_credor, '')) != ''
        then 'OUTRO'
    end as tipo_documento_credor,
    -- Brazilian format, right-aligned inside quotes ("           2.983.161,79"): strip
    -- the thousands separator, then turn the decimal comma into a point. This is the
    -- fifth number format in the dataset -- see the note in the PE legacy model.
    safe_cast(
        replace(replace(trim(empenhado), '.', ''), ',', '.') as float64
    ) as valor_empenhado,
    safe_cast(
        replace(replace(trim(liquidado), '.', ''), ',', '.') as float64
    ) as valor_liquidado,
    safe_cast(replace(replace(trim(pago), '.', ''), ',', '.') as float64) as valor_pago,
    -- Payments made this exercise against commitments carried over from earlier ones.
    -- Kept separate rather than folded into valor_pago, which would double-count
    -- against
    -- the year the commitment belongs to.
    safe_cast(
        replace(replace(trim(pago_restos), '.', ''), ',', '.') as float64
    ) as valor_pago_restos
from parsed
