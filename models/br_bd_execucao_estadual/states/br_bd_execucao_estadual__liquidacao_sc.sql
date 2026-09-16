{{ config(materialized="ephemeral") }}

-- Santa Catarina liquidações, at the liquidação-document level. Source: SIGEF via the
-- transparency portal (`visao=liquidacao`), 2011-2026, 10,295,205 rows.
--
-- SC carries the empenho of origin on every liquidação (`ugempenhooriginal`), so
-- `id_empenho_bd` joins `despesa`/`pagamento` natively on the composite key -- the same one
-- used throughout SC (`450022|2011NE000085`), never the bare document number, which
-- restarts per unidade gestora. Retenção is a separate document here, included as its own
-- row (see the note in `despesa_sc`).
--
-- id_liquidacao_bd is a Data Basis surrogate (the SC/PE/PB pattern): the liquidação key plus
-- the line's position within it, sequenced within (ugliquidacao, nunotaliquidacao) so a
-- monthly reload cannot renumber another.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_liquidacao") }}
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
    concat(
        'SC-', trim(ugliquidacao), '|', trim(nunotaliquidacao), '-',
        row_number() over (
            partition by trim(ugliquidacao), trim(nunotaliquidacao)
            order by
                trim(nunotaempenhooriginal),
                vlliquidacao,
                trim(dtlancamento),
                trim(nuidentificacao)
        )
    ) as id_liquidacao_bd,
    nullif(trim(nunotaliquidacao), '') as numero_liquidacao,
    case
        when nullif(trim(ugempenhooriginal), '') is not null
        then concat('SC-', trim(ugempenhooriginal))
    end as id_empenho_bd,
    nullif(trim(nunotaempenhooriginal), '') as numero_empenho,
    nullif(trim(nmunidadegestora), '') as nome_unidade_gestora,
    nullif(trim(nuidentificacao), '') as documento_credor,
    nullif(trim(nmcredor), '') as nome_credor,
    nullif(trim(deobservacao), '') as descricao,
    -- Comma decimal, no thousands separator.
    safe_cast(replace(vlliquidacao, ',', '.') as float64) as valor_liquidado
from fonte
where nullif(trim(nunotaliquidacao), '') is not null
