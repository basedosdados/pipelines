{{ config(materialized="ephemeral") }}

-- Ceará payments (Notas de Pagamento Direto), mapped onto the canonical `pagamento`
-- schema. Source: Portal da Transparência do Ceará, the pagamento exports, 2012-2026.
-- 7,792,663 rows in three naming eras (constants.CE_SCHEMAS['pagamento']): legacy
-- 2012-2016 (`num_ano`, `cod_*`, `vlr_*`), mid 2017 (`exercicio`, `numero`, bank
-- fields)
-- and portal 2018+ (`unidade_gestora`, `valor`). Each row populates one era's
-- columns, so
-- every field is a COALESCE across the eras.
--
-- GRAIN: one payment movement, the same as PE and SC -- the ledger includes anulações
-- and
-- non-settled statuses, and the sum of `valor_pago` is NOT required to equal
-- `despesa.valor_pago`.
--
-- **id_empenho_bd is NULL for CE, measured, not omitted.** The portal era --
-- 4,489,489 of
-- 7,792,663 rows -- carries no empenho number at all (0.6% populated), and where
-- legacy and
-- mid do carry one, CE's payment gestora code system does not resolve to the
-- empenho's (the
-- same mismatch that blocks the liquidação link: only 120 of 982 gestora codes
-- overlap and
-- the empenho número fans out ~93x). Emitting a link would resolve to the wrong
-- empenho, so
-- the column is null and `numero_empenho` carries the raw reference where present (the
-- PB-pagamento precedent).
--
-- **id_pagamento_bd is a Data Basis surrogate** (PE/SC/PB pattern): the payment key
-- plus a
-- sequence within it. `(exercicio, gestora, numero)` is unique for the portal and mid
-- eras
-- but repeats ~1.8% in the legacy era, so the row_number guarantees uniqueness; it is
-- sequenced within (exercicio, gestora, numero) so a reload cannot renumber another
-- key.
--
-- **valor_pago is NET of the row's own anulação column** (legacy vlr_pagamento -
-- vlr_anulacao_pagamento; mid/portal valor - valoranulado), and decimals parse with
-- `replace(',', '.')` -- CE never carries a thousands separator.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.ce_pagamento") }}
    ),
    base as (
        select
            coalesce(nullif(trim(exercicio), ''), nullif(trim(num_ano), '')) as exe,
            coalesce(
                nullif(trim(unidade_gestora), ''),
                nullif(trim(unidadegestora), ''),
                nullif(trim(cod_gestora), '')
            ) as ug,
            coalesce(nullif(trim(numero), ''), nullif(trim(cod_np), '')) as np,
            coalesce(
                nullif(trim(numeroned), ''), nullif(trim(cod_ne), '')
            ) as numero_empenho,
            coalesce(
                safe.parse_date('%Y-%m-%d', substr(trim(dth_movimento), 1, 10)),
                safe.parse_date('%Y-%m-%d', substr(trim(dataemissao), 1, 10)),
                safe.parse_date('%Y-%m-%d', substr(trim(data_emissao), 1, 10)),
                safe.parse_date('%d/%m/%Y', trim(dataemissao)),
                safe.parse_date('%d/%m/%Y', trim(data_emissao))
            ) as data,
            -- status: legacy a code, the later eras a bank-movement status label.
            coalesce(
                nullif(trim(status_movimento_bancario), ''),
                nullif(trim(statusmovbancario), ''),
                nullif(trim(cod_sit_pagamento), '')
            ) as situacao,
            nullif(trim(unidade_executora), '') as nome_ug,
            -- creditor document: legacy num_cpf_cpj, mid cpfcnpjcredor/documentocredor,
            -- portal documento_credor.
            coalesce(
                nullif(trim(documento_credor), ''),
                nullif(trim(cpfcnpjcredor), ''),
                nullif(trim(documentocredor), ''),
                nullif(trim(num_cpf_cpj), '')
            ) as doc_credor,
            coalesce(
                nullif(trim(nomecredor), ''),
                nullif(trim(credor), ''),
                nullif(trim(dsc_credor), '')
            ) as nome_credor,
            nullif(trim(justificativa), '') as descricao,
            -- net paid: legacy vlr_pagamento - vlr_anulacao_pagamento; later eras
            -- valor - valoranulado. valoranulado is empty in the portal era.
            coalesce(
                safe_cast(replace(vlr_pagamento, ',', '.') as float64) - coalesce(
                    safe_cast(replace(vlr_anulacao_pagamento, ',', '.') as float64), 0
                ),
                safe_cast(replace(valor, ',', '.') as float64)
                - coalesce(safe_cast(replace(valoranulado, ',', '.') as float64), 0)
            ) as valor_pago
        from fonte
        where coalesce(nullif(trim(numero), ''), nullif(trim(cod_np), '')) is not null
    )
select
    safe_cast(exe as int64) as ano,
    extract(month from data) as mes,
    data as data,
    'CE' as sigla_uf,
    concat(
        'CE-',
        exe,
        '-',
        ug,
        '-',
        np,
        '-',
        row_number() over (
            partition by exe, ug, np
            order by numero_empenho, valor_pago, doc_credor, data
        )
    ) as id_pagamento_bd,
    np as numero_ordem_bancaria,
    -- See header: CE's payment gestora code does not resolve to the empenho, and the
    -- portal
    -- era carries no empenho number, so the link is left null rather than emitted
    -- wrong.
    safe_cast(null as string) as id_empenho_bd,
    numero_empenho as numero_empenho,
    situacao as situacao,
    nome_ug as nome_unidade_gestora,
    doc_credor as documento_credor,
    nome_credor as nome_credor,
    -- CE names one creditor per payment and does not repeat the empenho's own.
    safe_cast(null as string) as documento_credor_empenho,
    safe_cast(null as string) as nome_credor_empenho,
    descricao as descricao,
    valor_pago as valor_pago
from base
