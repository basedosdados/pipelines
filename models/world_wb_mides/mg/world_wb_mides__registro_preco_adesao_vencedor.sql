-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="registro_preco_adesao_vencedor",
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
    -- The parent's `_bd` key is NOT in the staging mirror -- it is built by
    -- `world_wb_mides__registro_preco_adesao_item`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copy this replaces had already drifted
    -- from the parent's key, so the foreign key it published matched no parent
    -- row.
    --
    -- The join is scoped by municipality and exercise because `seq_item_reg_adesao`
    -- recurs across them; within a scope it determines the parent key
    -- (3,547,497 groups, 0 ambiguous, measured on the parquet 2026-09-24),
    -- so the join cannot fan out.
    p_registro_preco_adesao_item as (
        select
            id_municipio,
            ano,
            id_item_reg_adesao,
            min(id_registro_preco_adesao_item_bd) as id_registro_preco_adesao_item_bd
        from {{ ref("world_wb_mides__registro_preco_adesao_item") }}
        group by 1, 2, 3
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_venc_reg_adesao` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd,
            ' ',
            ifnull(t.num_doc_vencedor, ''),
            ' ',
            ifnull(t.seq_venc_reg_adesao, '')
        ) as string
    ) as id_registro_preco_adesao_vencedor_bd,
    safe_cast(
        p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd as string
    ) as id_registro_preco_adesao_item_bd,
    safe_cast(t.seq_venc_reg_adesao as string) as id_venc_reg_adesao,
    safe_cast(t.seq_item_reg_adesao as string) as id_item_reg_adesao,
    safe_cast(t.seq_reg_adesao as string) as id_reg_adesao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_vencedor as string) as numero_doc_vencedor,
    safe_cast(t.nom_vencedor as string) as nome_vencedor,
    safe_cast(t.valor_preco_unitario as float64) as valor_unitario,
    safe_cast(t.num_quant_licitado as string) as numero_quant_licitado,
    safe_cast(t.num_quant_aderido as string) as numero_quant_aderido,
    safe_cast(t.valor_pct_desconto as float64) as valor_percentual_desconto
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_registro_preco_adesao_vencedor_mg"
        )
    }} as t
left join
    p_registro_preco_adesao_item
    on t.id_municipio = p_registro_preco_adesao_item.id_municipio
    and safe_cast(t.ano as int64) = p_registro_preco_adesao_item.ano
    and t.seq_item_reg_adesao = p_registro_preco_adesao_item.id_item_reg_adesao
