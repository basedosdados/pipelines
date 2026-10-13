{{ config(materialized="ephemeral") }}

-- Bahia tender <-> empenho bridge.
--
-- BA links a tender to spending in two hops rather than one.
-- `VW_PROC_AQUISICAO_ITEM_INSTRUMENTO`
-- maps a tender item to a budget instrument (`NUM_INSTRUMENTO_ORCAMENTO`), and
-- `VW_PROCESSO_SEI` maps that instrument to the empenho it became. Joining them gives
-- the
-- same tender -> empenho relation MG publishes directly.
--
-- BA has no internal empenho surrogate id, so `id_empenho` is NULL and the empenho is
-- identified by `numero_empenho` -- the value that actually joins to `empenho_credor`.
-- The instrument is kept because it is the real key of the first hop and is what a
-- reader
-- needs to check the join.
--
-- The second hop only reaches 2019+, since that is where `VW_PROCESSO_SEI` starts.
-- Tender
-- items before then resolve to an instrument and stop there, which is why the
-- instrument
-- column is populated on rows whose numero_empenho is NULL rather than those rows being
-- dropped.
with
    instrumento as (
        select distinct processo_de_aquisicao, num_instrumento_orcamento
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.ba_licitacao_empenho"
                )
            }}
        where num_instrumento_orcamento is not null
    ),
    -- One instrument can carry several empenhos; that is a genuine one-to-many and is
    -- kept
    -- as separate rows rather than collapsed, exactly as MG's bridge is.
    empenho as (
        select distinct num_instrumento_orcamento, num_empenho_orcamento
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.ba_empenho_sei") }}
        where num_instrumento_orcamento is not null
    )

select
    'BA' as sigla_uf,
    concat('BA-', i.processo_de_aquisicao) as id_licitacao_bd,
    safe_cast(i.processo_de_aquisicao as string) as id_licitacao_origem,
    cast(null as string) as id_empenho,
    safe_cast(e.num_empenho_orcamento as string) as numero_empenho,
    safe_cast(i.num_instrumento_orcamento as string) as instrumento_orcamentario,
    cast(null as string) as dotacao_orcamentaria
from instrumento as i
left join empenho as e on i.num_instrumento_orcamento = e.num_instrumento_orcamento
