-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="licitacao_julgamento",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2031, "interval": 1},
        },
        cluster_by=["id_municipio"],
        labels={"tema": "economia"},
    )
}}
with
    -- seq_unidade -> cod_unidade. The procurement streams publish only the
    -- sequence form of the managing unit, and that is exactly what distinguishes
    -- rows sharing a process number. The spend streams publish both, so the code
    -- is recoverable. Measured: 9,319 pairs, 0 conflicts, 98.3% coverage.
    unidade_xwalk as (
        select distinct id_municipio, id_unidade_gestora, cod_unidade
        from
            (
                select id_municipio, id_unidade_gestora, cod_unidade
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_dotacao_mg"
                        )
                    }}
            )
    ),
    p_licitacao as (
        select distinct
            t.seq_licitacao,
            concat(
                t.orgao,
                ' ',
                ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                ' ',
                ifnull(t.num_processo, ''),
                ' ',
                ifnull(t.num_ano_processo, ''),
                ' ',
                ifnull(t.data_abert_proc_adm, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_licitacao_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
    ),
    p_licitacao_item as (
        select distinct
            t.seq_item_licitacao,
            concat(
                p_licitacao.id_licitacao_bd,
                ' ',
                ifnull(t.num_lote, ''),
                ' ',
                ifnull(t.num_item, '')
            ) as id_licitacao_item_bd
        from
            {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_item_mg") }}
            as t
        left join p_licitacao on t.seq_licitacao = p_licitacao.seq_licitacao
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_julgamento` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_licitacao_item.id_licitacao_item_bd,
            ' ',
            ifnull(t.num_doc_licitante, ''),
            ' ',
            ifnull(t.seq_julgamento, '')
        ) as string
    ) as id_licitacao_julgamento_bd,
    safe_cast(p_licitacao_item.id_licitacao_item_bd as string) as id_licitacao_item_bd,
    safe_cast(t.seq_julgamento as string) as id_julgamento,
    safe_cast(t.seq_item_licitacao as string) as id_item_licitacao,
    safe_cast(t.seq_licitacao as string) as id_licitacao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_lote as string) as numero_lote,
    safe_cast(t.dsc_lote as string) as lote,
    safe_cast(t.num_item as string) as numero_item,
    safe_cast(t.dsc_item as string) as item,
    safe_cast(t.valor_unitario as float64) as valor_unitario,
    safe_cast(t.num_quant_item as string) as numero_quant_item,
    safe_cast(t.valor_perc_desconto as float64) as valor_perc_desconto,
    safe_cast(t.valor_perc_taxa_adm as float64) as valor_perc_taxa_adm,
    safe_cast(t.valor_global as float64) as valor_global,
    safe_cast(t.dsc_ind_desonera_folha as string) as ind_desonera_folha,
    safe_cast(t.num_doc_licitante as string) as numero_doc_licitante,
    safe_cast(t.nom_licitante as string) as nome_licitante
from
    {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_julgamento_mg") }}
    as t
left join p_licitacao_item on t.seq_item_licitacao = p_licitacao_item.seq_item_licitacao
