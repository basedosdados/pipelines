-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_credito",
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
    -- sequence. `seq_credito_contrato` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_contrato.id_contrato_bd,
            ' ',
            ifnull(t.dsc_dotacao, ''),
            ' ',
            ifnull(t.dsc_fonte_recurso, ''),
            ' ',
            ifnull(t.seq_credito_contrato, '')
        ) as string
    ) as id_contrato_credito_bd,
    safe_cast(p_contrato.id_contrato_bd as string) as id_contrato_bd,
    safe_cast(t.seq_credito_contrato as string) as id_credito_contrato,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_dotacao as string) as dotacao,
    safe_cast(t.dsc_funcao as string) as funcao,
    safe_cast(t.dsc_subfuncao as string) as subfuncao,
    safe_cast(t.dsc_programa as string) as programa,
    safe_cast(t.dsc_acao as string) as acao,
    safe_cast(t.dsc_subacao as string) as subacao,
    safe_cast(t.dsc_nat_despesa as string) as natureza_despesa,
    safe_cast(t.dsc_fonte_recurso as string) as fonte_recurso,
    safe_cast(t.valor_recurso as float64) as valor_recurso
from {{ set_datalake_project("world_wb_mides_staging.raw_contrato_credito_mg") }} as t
left join p_contrato on t.seq_contrato = p_contrato.seq_contrato
