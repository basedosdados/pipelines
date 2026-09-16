{{ config(materialized="ephemeral") }}

-- Paraíba payments, at the payment-document level (ordem cronológica de pagamentos).
--
-- Source: CGE-PB's REST API (`/despesas/ordem_cronologica_pagamentos`), 2015-2026.
--
-- **`id_empenho_bd` is deliberately NULL here, and that is a source limitation, not an
-- omission.** PB's payment ledger identifies the spending body by
-- `codigoUnidadeGestora`, which is a DIFFERENT code system from the
-- `codigoOrgao` / `codigoUnidade` pair on the empenho:
--
-- empenho 2024-03      93 órgãos (e.g. 250001), 118 unidades (e.g. 25101)
-- liquidações          42 códigoOrgao   -> 42 of 42 match the empenho's órgãos
-- pagamentos           17 codigoUnidadeGestora -> 6 match an órgão, **0** match a
-- unidade
--
-- So liquidação joins to the empenho cleanly and pagamento does not. `numeroEmpenho`
-- alone cannot carry the join: it is an integer that restarts per exercise and per
-- unit, and in one month of 2024 it fans out roughly eightfold. Emitting a key built
-- from a code system that does not match would produce a link that looks real and
-- resolves to the wrong empenho, so the column is left null until PB publishes a
-- UG <-> órgão crosswalk.
--
-- The table is still worth having: it carries the payment date, the creditor, the
-- value, the chronological position and the authorisation status, none of which the
-- empenho row has. `despesa.valor_pago` remains the per-empenho total.
--
-- As with PE and SC, summing `valor_pago` here is NOT required to equal
-- `despesa.valor_pago`: this is the movement ledger, and it includes entries at every
-- authorisation status.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pb_pagamento") }}
    )
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    -- `dataExecucao` is when the payment was actually executed; `dataInsercao` is when
    -- it entered the queue. The executed date is the one comparable to the other
    -- states' payment dates, and it falls back to the insertion date when the payment
    -- is still queued.
    coalesce(
        safe.parse_date('%Y-%m-%d', substr(trim(dataexecucao), 1, 10)),
        safe.parse_date('%Y-%m-%d', substr(trim(datainsercao), 1, 10))
    ) as data,
    'PB' as sigla_uf,
    -- One `numeroap` (ordem de pagamento) settles many liquidação lines, so
    -- `<ano>-<ug>-<numeroap>` alone repeats (311,853 excess over 2015-2026). There is no
    -- byte-identical duplicate row -- the composite
    -- (ano, ug, numeroap, liquidacao, numeroempenho, valorpago, cnpjcpfcredor) is exactly
    -- unique -- so the id is a Data Basis surrogate in the PE/SC pattern: the payment key
    -- plus the line's position within it, sequenced WITHIN (ano, ug, numeroap) so
    -- reloading one month cannot renumber another (a numeroap is confined to one month).
    concat(
        'PB-', trim(ano), '-', trim(codigounidadegestora), '-', trim(numeroap), '-',
        row_number() over (
            partition by trim(ano), trim(codigounidadegestora), trim(numeroap)
            order by
                trim(liquidacao),
                trim(numeroempenho),
                valorpago,
                trim(cnpjcpfcredor),
                trim(posicao),
                trim(datainsercao)
        )
    ) as id_pagamento_bd,
    nullif(trim(numeroap), '') as numero_ordem_bancaria,
    -- See the header note: PB's payment unit code does not resolve to the empenho's.
    safe_cast(null as string) as id_empenho_bd,
    nullif(trim(numeroempenho), '') as numero_empenho,
    nullif(trim(status), '') as situacao,
    nullif(trim(descricaounidadegestora), '') as nome_unidade_gestora,
    nullif(trim(cnpjcpfcredor), '') as documento_credor,
    nullif(trim(nomecredor), '') as nome_credor,
    -- PB names one creditor per payment and does not repeat the empenho's own.
    safe_cast(null as string) as documento_credor_empenho,
    safe_cast(null as string) as nome_credor_empenho,
    nullif(trim(justificativa), '') as descricao,
    safe_cast(valorpago as float64) as valor_pago
from fonte
where nullif(trim(numeroap), '') is not null
