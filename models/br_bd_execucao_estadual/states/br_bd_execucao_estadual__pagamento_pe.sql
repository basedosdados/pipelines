{{ config(materialized="ephemeral") }}

-- Pernambuco payment orders (ordens bancárias), mapped onto the canonical `pagamento`
-- schema. Source: e-Fisco `all-pagamentos`, 19 annual files, 10,710,893 rows from 2008.
--
-- This is the one source in the dataset that publishes execution at PAYMENT level. MG
-- publishes only a `vr_pago` column on the expense row and a two-column payment-status
-- link (`fl_despesa_pgto`: a payment sequence and a status, with no value, date or
-- empenho), BA publishes no payment document at all, and SP is annual. So `pagamento`
-- is PE-only, and it is what makes the empenho -> payment chain real for one state
-- rather than a column total.
--
-- What it adds over `despesa.valor_pago`: an actual payment DATE. PE's expense export
-- carries no date and no month from 2011 on, so for 2011+ the only sub-annual timing
-- available for PE anywhere in this dataset is here.
--
-- GRAIN: one payment line. A single ordem bancária pays MANY empenhos -- 1,702,822
-- distinct OB numbers across 10,710,893 lines -- so the OB number alone is 84%
-- non-unique and is NOT the key. Within one (OB, empenho) the line can still repeat
-- with a different value, creditor or purpose. The full row is unique across all
-- 10,710,893 rows, with no exact duplicates.
select
    safe_cast(t.ano as int64) as ano,
    extract(month from safe.parse_date('%Y-%m-%d', trim(t.dt_lancamento))) as mes,
    safe.parse_date('%Y-%m-%d', trim(t.dt_lancamento)) as data,
    'PE' as sigla_uf,
    -- No natural key exists, so the id is a Data Basis surrogate: the ordem bancária
    -- plus the line's position within it. Sequenced within the OB rather than across
    -- the
    -- table so that reloading one exercise cannot renumber another.
    --
    -- 20 rows of 2025 carry an empenho, a value and a unidade gestora but NO ordem
    -- bancária, status or date -- payments authorised and not yet issued. They hold
    -- real
    -- money, so they are kept rather than dropped, under a SEMOB marker. The partition
    -- below is the same expression as the id, so the two cannot diverge.
    concat(
        'PE-',
        coalesce(nullif(trim(t.num_ordem_bancaria), ''), 'SEMOB'),
        '-',
        row_number() over (
            partition by coalesce(nullif(trim(t.num_ordem_bancaria), ''), 'SEMOB')
            order by
                t.numero_empenho,
                t.vlr_ordem_bancaria,
                t.credor_ordem_bancaria,
                t.finalidade_ordem_bancaria
        )
    ) as id_pagamento_bd,
    safe_cast(
        nullif(trim(t.num_ordem_bancaria), '') as string
    ) as numero_ordem_bancaria,
    -- Joins to despesa.id_empenho_bd. The exercise stamped on the payment file is the
    -- right half of the key, not the year embedded in the empenho number: 4.9% of
    -- payments settle an empenho from an earlier exercise (restos a pagar), and PE
    -- republishes those empenhos in the paying exercise's expense file too, so this
    -- form
    -- matches 100.0% of the 1,166,130 distinct payment keys. Joining on the empenho
    -- number alone instead matches 135% -- the same number exists under several
    -- exercises -- and fans out.
    case
        when trim(t.numero_empenho) != ''
        then concat('PE', t.ano, '-', trim(t.numero_empenho))
    end as id_empenho_bd,
    safe_cast(nullif(trim(t.numero_empenho), '') as string) as numero_empenho,
    -- NOT every row is money that left the treasury: PAGA is 97.8%, but DEVOLVIDA,
    -- CANCELADA, DEVOLVIDA APOS PAGTO, AJUSTADA, ENVIADA and GERADA also appear. Filter
    -- on this before summing `valor_pago`.
    safe_cast(nullif(trim(t.situacao), '') as string) as situacao,
    safe_cast(nullif(trim(t.unidade_gestora), '') as string) as nome_unidade_gestora,
    -- Two creditors, and they differ on 49.9% of rows: `credor_empenho` is who the
    -- commitment named, `credor_ordem_bancaria` is who the bank order actually paid.
    -- Both are kept; neither is derivable from the other.
    --
    -- Same "<document> - <name>" shape as the expense export, and the document is taken
    -- as everything before the first " - " because a masked CPF contains a dash. It is
    -- not always a document: PE also issues pseudo-codes for non-CNPJ payees
    -- ("PF20090902 - DEPOSITOS JUDICIAIS DE VARAS ESTADUAIS - TJPE").
    safe_cast(
        nullif(regexp_extract(t.credor_ordem_bancaria, r'^(.*?) - '), '') as string
    ) as documento_credor,
    safe_cast(
        nullif(regexp_extract(t.credor_ordem_bancaria, r'^.*? - (.*)$'), '') as string
    ) as nome_credor,
    safe_cast(
        nullif(regexp_extract(t.credor_empenho, r'^(.*?) - '), '') as string
    ) as documento_credor_empenho,
    safe_cast(
        nullif(regexp_extract(t.credor_empenho, r'^.*? - (.*)$'), '') as string
    ) as nome_credor_empenho,
    safe_cast(nullif(trim(t.finalidade_ordem_bancaria), '') as string) as descricao,
    safe_cast(trim(t.vlr_ordem_bancaria) as float64) as valor_pago
from {{ set_datalake_project("br_bd_execucao_estadual_staging.pe_pagamento") }} as t
