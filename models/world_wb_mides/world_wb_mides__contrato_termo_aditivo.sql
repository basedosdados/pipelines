-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_termo_aditivo",
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
    -- `world_wb_mides__contrato`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copies this replaces had already
    -- drifted -- they predated the `seq_contrato`/`seq_dispensa` tie-breakers,
    -- so the `id_contrato_bd` they published matched no row of the parent.
    --
    -- The join is scoped by municipality and sequence, NOT by exercise. A
    -- child routinely cites a parent recorded in an earlier exercise, so
    -- matching on the child's own `ano` discards those references.
    -- Here it left `_bd` NULL for 695,953 of 966,014 rows (72.04%),
    -- against 59,413 (6.15%) once the exercise is out of the join.
    --
    -- Dropping it cannot fan out: `(id_municipio, seq_contrato)` spans more than
    -- one exercise in 0 of the parent's groups, and the `min` below would
    -- collapse any future ambiguity to one key deterministically.
    -- Measured on the staging parquet with DuckDB, 2026-09-28.
    p_contrato as (
        select id_municipio, id_contrato, min(id_contrato_bd) as id_contrato_bd
        from {{ ref("world_wb_mides__contrato") }}
        group by 1, 2
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_contrato.id_contrato_bd, ' ', ifnull(t.num_termo_aditivo, '')
        ) as string
    ) as id_contrato_termo_aditivo_bd,
    safe_cast(p_contrato.id_contrato_bd as string) as id_contrato_bd,
    safe_cast(t.seq_termo_aditivo as string) as id_termo_aditivo,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_contrato as string) as numero_contrato,
    safe_cast(t.num_ano_contrato as string) as numero_ano_contrato,
    safe_cast(t.data_assinatura as date) as data_assinatura,
    safe_cast(t.num_termo_aditivo as string) as numero_termo_aditivo,
    safe_cast(t.data_ass_termo as date) as data_ass_termo,
    safe_cast(t.dsc_tipo_termo as string) as tipo_termo,
    safe_cast(t.dsc_tipo_alteracao as string) as tipo_alteracao,
    safe_cast(t.dsc_alteracao as string) as alteracao,
    safe_cast(t.data_novo_termino as date) as data_novo_termino,
    safe_cast(t.data_publicacao as date) as data_publicacao,
    safe_cast(t.dsc_veiculo_pub as string) as veiculo_publicacao,
    safe_cast(t.valor_termo_aditivo as float64) as valor_termo_aditivo
from
    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_termo_aditivo_mg") }}
    as t
left join
    p_contrato
    on t.id_municipio = p_contrato.id_municipio
    and t.seq_contrato = p_contrato.id_contrato
