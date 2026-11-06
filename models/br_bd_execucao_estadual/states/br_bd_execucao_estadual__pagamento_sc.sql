{{ config(materialized="ephemeral") }}

-- Santa Catarina payments, at the payment-document level.
--
-- Source: SIGEF via the transparency portal's export endpoint (`visao=pagamento`),
-- 2011-2026. SC is the second state in this table, after Pernambuco: it is one of only
-- two sources here that publish a payment document at all, and the only one that links
-- it to BOTH the liquidação and the empenho natively.
--
-- Every row is one payment movement. The three types are distinct documents, never
-- components of one another (2024-03: 50,965 Líquido, 22,877 Retenção, 448 Estorno,
-- and zero documents carrying more than one type):
--
-- * **Líquido** -- the amount paid to the creditor.
-- * **Retenção** -- tax or charge withheld from the same liquidação and paid to a third
-- party. It is a real disbursement against the empenho, which is why `despesa`
-- counts it; see the note in `despesa_sc`.
-- * **Estorno** -- a correction, **already negative** in the source.
--
-- The type is carried in `situacao` so a reader can pick. As with PE, the sum of
-- `valor_pago` here is NOT required to equal `despesa.valor_pago` for the same empenho
-- once filtered: that column is the empenho's total, this table is the individual
-- movements.
--
-- **Join on the composite key, never on the bare document number.** SC restarts
-- numbering per unidade gestora -- 19,533 empenho numbers in 2011 alone appear under
-- two or more UGs -- so `ugempenhooriginal` (`450022|2011NE000085`) is the empenho key
-- and `cdunidadegestora || '|' || nupagamento` is the payment key.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_pagamento") }}
    )
select
    extract(
        year
        from safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(trim(dtlancamento), 1, 19))
    ) as ano,
    extract(
        month
        from safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(trim(dtlancamento), 1, 19))
    ) as mes,
    date(
        safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(trim(dtlancamento), 1, 19))
    ) as data,
    'SC' as sigla_uf,
    -- SC restarts payment numbering per unidade gestora, and one `nupagamento` covers
    -- several liquidação lines, so `<ug>|<nupagamento>` alone repeats (30,909 excess
    -- over
    -- 2011-2026, all with a distinct nunotaliquidacao or value, none a duplicate
    -- row). No
    -- natural line id exists: the composite
    -- (ug, nupagamento, nunotaliquidacao, nuidentificacao, vlpagamento, dtlancamento)
    -- is
    -- exactly unique across all 13.3M rows, so the id is a Data Basis surrogate in
    -- the PE
    -- pattern -- the payment key plus the line's position within it, sequenced WITHIN
    -- (ug, nupagamento) so reloading one month cannot renumber another (a nupagamento
    -- is
    -- confined to a single date).
    concat(
        'SC-',
        trim(cdunidadegestora),
        '|',
        trim(nupagamento),
        '-',
        row_number() over (
            partition by trim(cdunidadegestora), trim(nupagamento)
            order by
                trim(nunotaliquidacao),
                trim(nunotaempenhooriginal),
                trim(nmtipopagamento),
                vlpagamento,
                trim(dtlancamento),
                trim(nuidentificacao)
        )
    ) as id_pagamento_bd,
    nullif(trim(nuordembancaria), '') as numero_ordem_bancaria,
    concat('SC-', trim(ugempenhooriginal)) as id_empenho_bd,
    nullif(trim(nunotaempenhooriginal), '') as numero_empenho,
    -- Líquido / Retenção / Estorno. PE puts a payment status here; SC has no status,
    -- and the movement type is the distinction a reader actually needs to filter on.
    nullif(trim(nmtipopagamento), '') as situacao,
    nullif(trim(nmunidadegestora), '') as nome_unidade_gestora,
    nullif(trim(nuidentificacao), '') as documento_credor,
    nullif(trim(nmcredor), '') as nome_credor,
    -- SC names one creditor per payment: the payee of THIS document, which for a
    -- retenção is the third party rather than the empenho's supplier. The source does
    -- not repeat the empenho's own creditor on the payment row, and carrying the payee
    -- across into these columns would assert an identity the source never made.
    safe_cast(null as string) as documento_credor_empenho,
    safe_cast(null as string) as nome_credor_empenho,
    coalesce(
        nullif(trim(deobservacaopp), ''),
        nullif(trim(deobservacaonl), ''),
        nullif(trim(dehistoricoempenho), '')
    ) as descricao,
    -- Comma decimal, no thousands separator. Estorno rows are already negative.
    safe_cast(replace(vlpagamento, ',', '.') as float64) as valor_pago
from fonte
where nullif(trim(nupagamento), '') is not null
