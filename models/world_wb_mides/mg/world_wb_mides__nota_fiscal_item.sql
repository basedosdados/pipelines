-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="nota_fiscal_item",
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
    p_nota_fiscal as (
        select distinct
            t.seq_nota_fiscal,
            concat(
                t.orgao,
                ' ',
                ifnull(t.num_doc_emitente, ''),
                ' ',
                ifnull(t.num_nota_fiscal, ''),
                ' ',
                ifnull(t.num_serie_nota_fiscal, ''),
                ' ',
                ifnull(t.data_emissao, ''),
                ' ',
                ifnull(t.cod_chave_nota_fis, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_nota_fiscal_bd
        from
            {{ set_datalake_project("world_wb_mides_staging.raw_nota_fiscal_mg") }} as t
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_nota_fiscal.id_nota_fiscal_bd,
            ' ',
            ifnull(t.cod_item, ''),
            ' ',
            ifnull(t.dsc_item, '')
        ) as string
    ) as id_nota_fiscal_item_bd,
    safe_cast(p_nota_fiscal.id_nota_fiscal_bd as string) as id_nota_fiscal_bd,
    safe_cast(t.seq_item_nota as string) as id_item_nota,
    safe_cast(t.seq_nota_fiscal as string) as id_nota_fiscal,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.cod_item as string) as codigo_item,
    safe_cast(t.dsc_item as string) as item,
    safe_cast(t.dsc_unid_medida as string) as unid_medida,
    safe_cast(t.num_quant_item as string) as numero_quant_item,
    safe_cast(t.valor_item as float64) as valor_item
from {{ set_datalake_project("world_wb_mides_staging.raw_nota_fiscal_item_mg") }} as t
left join p_nota_fiscal on t.seq_nota_fiscal = p_nota_fiscal.seq_nota_fiscal
