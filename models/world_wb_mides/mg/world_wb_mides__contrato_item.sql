-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_item",
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
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_item_contrato` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_contrato.id_contrato_bd,
            ' ',
            ifnull(t.cod_item, ''),
            ' ',
            ifnull(t.num_item_planilha, ''),
            ' ',
            ifnull(t.seq_item_contrato, '')
        ) as string
    ) as id_contrato_item_bd,
    safe_cast(p_contrato.id_contrato_bd as string) as id_contrato_bd,
    safe_cast(t.seq_item_contrato as string) as id_item_contrato,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.cod_item as string) as codigo_item,
    safe_cast(t.dsc_item as string) as item,
    safe_cast(t.dsc_unid_medida as string) as unid_medida,
    safe_cast(t.dsc_tipo_material_serv as string) as tipo_material_servico,
    safe_cast(t.cod_item_sinapi as string) as codigo_item_sinapi,
    safe_cast(t.cod_item_sicro as string) as codigo_item_sicro,
    safe_cast(t.dsc_tabela as string) as tabela,
    safe_cast(t.num_item_planilha as string) as numero_item_planilha,
    safe_cast(t.num_quant_item as string) as numero_quant_item,
    safe_cast(t.valor_item as float64) as valor_item
from {{ set_datalake_project("world_wb_mides_staging.raw_contrato_item_mg") }} as t
left join p_contrato on t.seq_contrato = p_contrato.seq_contrato
