-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_apostilamento",
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
    safe_cast(
        concat(
            p_contrato.id_contrato_bd, ' ', ifnull(t.num_apostilamento, '')
        ) as string
    ) as id_contrato_apostilamento_bd,
    safe_cast(p_contrato.id_contrato_bd as string) as id_contrato_bd,
    safe_cast(t.seq_apostilamento as string) as id_apostilamento,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_tipo_apost as string) as tipo_apost,
    safe_cast(t.num_apostilamento as string) as numero_apostilamento,
    safe_cast(t.data_apostilamento as date) as data_apostilamento,
    safe_cast(t.dsc_tipo_alteracao as string) as tipo_alteracao,
    safe_cast(t.dsc_alteracao as string) as alteracao,
    safe_cast(t.valor_apostilamento as float64) as valor_apostilamento
from
    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_apostilamento_mg") }}
    as t
left join p_contrato on t.seq_contrato = p_contrato.seq_contrato
