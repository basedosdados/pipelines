-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_termo_aditivo_item",
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
    p_contrato as (
        select distinct
            t.seq_contrato,
            concat(
                t.orgao,
                ' ',
                ifnull(t.cod_unidade, ''),
                ' ',
                ifnull(t.cod_subunidade, ''),
                ' ',
                ifnull(t.num_contrato, ''),
                ' ',
                ifnull(t.num_ano_contrato, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_contrato_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }} as t
    ),
    p_contrato_termo_aditivo as (
        select distinct
            t.seq_termo_aditivo,
            concat(
                p_contrato.id_contrato_bd, ' ', ifnull(t.num_termo_aditivo, '')
            ) as id_contrato_termo_aditivo_bd
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_contrato_termo_aditivo_mg"
                )
            }} as t
        left join p_contrato on t.seq_contrato = p_contrato.seq_contrato
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_item_termo` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_contrato_termo_aditivo.id_contrato_termo_aditivo_bd,
            ' ',
            ifnull(t.cod_item, ''),
            ' ',
            ifnull(t.num_item_planilha, ''),
            ' ',
            ifnull(t.seq_item_termo, '')
        ) as string
    ) as id_contrato_termo_aditivo_item_bd,
    safe_cast(
        p_contrato_termo_aditivo.id_contrato_termo_aditivo_bd as string
    ) as id_contrato_termo_aditivo_bd,
    safe_cast(t.seq_item_termo as string) as id_item_termo,
    safe_cast(t.seq_termo_aditivo as string) as id_termo_aditivo,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.cod_item as string) as codigo_item,
    safe_cast(t.dsc_item as string) as item,
    safe_cast(t.dsc_unid_medida as string) as unid_medida,
    safe_cast(t.dsc_tipo_mat_serv as string) as tipo_material_servico,
    safe_cast(t.cod_item_sinapi as string) as codigo_item_sinapi,
    safe_cast(t.cod_item_sicro as string) as codigo_item_sicro,
    safe_cast(t.dsc_tabela as string) as tabela,
    safe_cast(t.num_item_planilha as string) as numero_item_planilha,
    safe_cast(t.dsc_tipo_alteracao as string) as tipo_alteracao,
    safe_cast(t.num_quant_acres_decres as string) as numero_quant_acres_decres,
    safe_cast(t.valor_unitario as float64) as valor_unitario
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_contrato_termo_aditivo_item_mg"
        )
    }} as t
left join
    p_contrato_termo_aditivo
    on t.seq_termo_aditivo = p_contrato_termo_aditivo.seq_termo_aditivo
