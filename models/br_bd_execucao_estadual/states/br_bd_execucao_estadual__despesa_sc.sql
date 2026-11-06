{{ config(materialized="ephemeral") }}

-- Santa Catarina execution, PIVOTED onto the canonical `despesa` schema.
--
-- Source: SIGEF via the transparency portal's own export endpoint, 2011-2026. See
-- code/download_sc.py for why the CKAN bulk files are not used.
--
-- **SC publishes one row per MOVEMENT, like RS**, so it is pivoted here for the same
-- reason: MG, BA, PE, SP and ES all put `empenhado` / `liquidado` / `pago` side by side
-- on one row, and pivoting only aggregates rows that already exist, while unpivoting
-- would invent documents and dates the source never published.
--
-- Two separate pivots happen, and they are not the same operation:
--
-- 1. **Within the empenho.** `cdtipoempenho` is Emissão / Reforço / Anulação / Estorno,
-- and each is its own document with its own `nunotaempenho`. A modification points at
-- what it modifies through `nunotaempenhooriginal` / `ugempenhooriginal`. In 2011 that
-- is 162,581 emissões plus 47,527 modifications, **100% of which resolve to an
-- emissão**, and grouping on the original returns exactly 162,581 logical empenhos.
-- `vlempenho` is **already signed** -- anulação and estorno are negative in the source
-- -- so the net is a plain SUM and must not be built from absolute values.
-- 2. **Across phases.** Liquidação and pagamento are separate documents that carry
-- `nunotaempenhooriginal`, so they aggregate onto the empenho natively. Only PB
-- otherwise publishes the chain this way; MG, BA, PE, SP and ES have none of it.
--
-- **The empenho number is NOT unique on its own.** In 2011 there are 210,108 rows over
-- only 21,753 distinct `nunotaempenho`, and 19,533 of those numbers appear under two or
-- more unidades gestoras -- the numbering restarts per UG. Every join here is therefore
-- on the composite `ugempenho` (`450022|2011NE000085`), never on `nunotaempenho`.
-- Joining on the bare number would multiply rows roughly tenfold.
--
-- SC contributes no `licitacao*`: the portal publishes a tender *modality* on the
-- expense row and no tender table, so `id_licitacao*` are null rather than emitted from
-- something that would leave every key dangling (the RS decision, not the ES one).
with
    empenho as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_empenho") }}
    ),
    movimento as (
        select
            -- The logical empenho: a modification belongs to the document it modifies,
            -- an emissão to itself. Keyed on the composite, per the note above.
            case
                when nullif(trim(nunotaempenhooriginal), '') is null
                then trim(ugempenho)
                else trim(ugempenhooriginal)
            end as id_empenho,
            safe_cast(cdtipoempenho as int64) as tipo_movimento,
            trim(ugempenho) as ugempenho,
            trim(nunotaempenho) as numero_empenho,
            safe.parse_datetime(
                '%Y-%m-%d %H:%M:%S', substr(trim(dtlancamento), 1, 19)
            ) as lancamento,
            -- Comma decimal, no thousands separator ('25742,5'), as in BA, ES and RS.
            -- Do NOT copy this to SP, where 76% of values carry a '.' thousands
            -- separator and this expression would null them.
            safe_cast(replace(vlempenho, ',', '.') as float64) as valor,
            trim(cdorgao) as cod_orgao,
            trim(nmorgao) as nome_orgao,
            trim(cdunidadegestora) as cod_ug,
            trim(nmunidadegestora) as nome_ug,
            trim(cdfuncao) as cod_funcao,
            trim(cdsubfuncao) as cod_subfuncao,
            trim(cdprograma) as cod_programa,
            trim(cdacao) as cod_acao,
            -- `cdsubelemento` is the full 8-digit natureza da despesa, and is 8 digits
            -- on every one of the 210,108 rows checked, so the split is safe:
            -- C G MM EE SS = categoria, grupo, modalidade de aplicação, elemento,
            -- subelemento. SC publishes no separate columns for these.
            trim(cdsubelemento) as cod_natureza,
            trim(cdfonterecurso) as cod_fonte,
            nullif(trim(demodalidadeempenho), '') as tipo_empenho,
            nullif(trim(nmmodalidade), '') as modalidade_licitacao,
            nullif(trim(nuidentificacao), '') as documento_credor,
            nullif(trim(nmcredor), '') as nome_credor,
            nullif(trim(dehistoricoempenho), '') as descricao
        from empenho
        where nullif(trim(ugempenho), '') is not null
    ),
    -- The emissão carries the dimensions, the creditor and the date. A modification
    -- repeats most of them, but taking them from the emissão is what makes the row mean
    -- "this commitment", and it keeps a 2020 commitment cancelled in 2021 in 2020.
    cabecalho as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_empenho
                        order by
                            case when tipo_movimento = 1 then 0 else 1 end, lancamento
                    ) as rn
                from movimento
            )
        where rn = 1
    ),
    empenhado as (
        select id_empenho, sum(valor) as valor_empenhado
        from movimento
        group by id_empenho
    ),
    liquidado as (
        select
            trim(ugempenhooriginal) as id_empenho,
            sum(
                safe_cast(replace(vlliquidacao, ',', '.') as float64)
            ) as valor_liquidado
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_liquidacao") }}
        where nullif(trim(ugempenhooriginal), '') is not null
        group by 1
    ),
    -- **Retenção is INCLUDED here, unlike in RS, and the difference is not an
    -- inconsistency.** RS books a retenção as a withholding taken from inside a
    -- payment it also reports in full, so adding it double-counts. SC does not: every
    -- `nupagamento` document is Líquido OR Retenção OR Estorno and never a mix
    -- (2024-03: 50,965 / 22,877 / 448 documents, zero mixed), so the Líquido document
    -- is the net paid to the creditor and the Retenção document is the complement paid
    -- to a third party.
    --
    -- The totals agree with that reading: for 2024-03, liquidação nets R$3.06bn while
    -- Líquido alone is R$2.42bn and Líquido + Retenção + Estorno is R$2.91bn. Summing
    -- only Líquido would understate what the empenho paid by about a fifth.
    --
    -- Estorno is already negative, so this is a plain SUM over every row.
    -- `validate_sc.py` re-checks this across the whole series rather than the one month
    -- it was established on.
    pago as (
        select
            trim(ugempenhooriginal) as id_empenho,
            sum(safe_cast(replace(vlpagamento, ',', '.') as float64)) as valor_pago
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_pagamento") }}
        where nullif(trim(ugempenhooriginal), '') is not null
        group by 1
    )
select
    extract(year from c.lancamento) as ano,
    extract(month from c.lancamento) as mes,
    date(c.lancamento) as data,
    'SC' as sigla_uf,
    c.cod_orgao as orgao,
    c.nome_orgao as nome_orgao,
    c.cod_ug as id_unidade_gestora,
    c.nome_ug as nome_unidade_gestora,
    concat('SC-', c.id_empenho) as id_empenho_bd,
    c.id_empenho as id_empenho,
    c.numero_empenho as numero_empenho,
    -- Ordinário / Estimativo / Global -- the classification the column is for, not the
    -- movement type, which the pivot above has already consumed.
    c.tipo_empenho as tipo_empenho,
    c.descricao as descricao,
    -- SC publishes no tender table, so there is nothing for a tender id to reach.
    safe_cast(null as string) as id_licitacao_bd,
    safe_cast(null as string) as id_licitacao,
    c.modalidade_licitacao as modalidade_licitacao,
    c.documento_credor as documento_credor,
    c.nome_credor as nome_credor,
    -- Derived from the published format, not inferred from content: SC prints companies
    -- as a formatted CNPJ (`20.603.864/0001-05`) and natural persons as a masked CPF
    -- (`***.094.449-**`, the MG convention). The two are structurally distinct.
    case
        when c.documento_credor like '%/%'
        then 'PJ'
        when c.documento_credor like '%*%'
        then 'PF'
    end as tipo_documento_credor,
    c.cod_funcao as funcao,
    c.cod_subfuncao as subfuncao,
    c.cod_programa as programa,
    c.cod_acao as acao,
    substr(c.cod_natureza, 1, 1) as categoria_economica,
    substr(c.cod_natureza, 2, 1) as grupo_despesa,
    substr(c.cod_natureza, 3, 2) as modalidade_aplicacao,
    substr(c.cod_natureza, 5, 2) as elemento_despesa,
    substr(c.cod_natureza, 7, 2) as item_despesa,
    c.cod_fonte as fonte_recurso,
    -- The document type IS the movement type in this source, and the pivot has just
    -- consumed it, so there is nothing left for this column to carry.
    safe_cast(null as string) as tipo_documento,
    e.valor_empenhado as valor_empenhado,
    l.valor_liquidado as valor_liquidado,
    p.valor_pago as valor_pago
from cabecalho as c
left join empenhado as e on c.id_empenho = e.id_empenho
left join liquidado as l on c.id_empenho = l.id_empenho
left join pago as p on c.id_empenho = p.id_empenho
