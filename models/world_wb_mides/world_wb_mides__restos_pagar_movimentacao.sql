-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="restos_pagar_movimentacao",
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
    -- The parent's key is built by `restos_pagar`; reading it from `ref()` rather
    -- than re-deriving the concat here means the two can never drift. (The raw
    -- mirror could not be used anyway: it carries no `ano`.)
    p_restos_pagar as (
        select distinct id_rsp, id_restos_pagar_bd
        from {{ ref("world_wb_mides__restos_pagar") }}
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_restos_pagar.id_restos_pagar_bd,
            ' ',
            ifnull(t.data_movimentacao, ''),
            ' ',
            ifnull(t.dsc_tipo_movimentacao, ''),
            ' ',
            ifnull(t.valor_movimentacao, ''),
            ' ',
            ifnull(t.dsc_tipo_rsp, '')
        ) as string
    ) as id_restos_pagar_movimentacao_bd,
    safe_cast(p_restos_pagar.id_restos_pagar_bd as string) as id_restos_pagar_bd,
    safe_cast(t.seq_mov_rsp as string) as id_mov_rsp,
    safe_cast(t.seq_rsp as string) as id_rsp,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.cod_unidade as string) as codigo_unidade,
    safe_cast(t.cod_subunidade as string) as codigo_subunidade,
    safe_cast(t.num_empenho_origem as string) as numero_empenho_origem,
    safe_cast(t.data_empenho_origem as date) as data_empenho_origem,
    safe_cast(t.num_ano_emp_origem as string) as numero_ano_emp_origem,
    safe_cast(t.dsc_dotacao_ori as string) as dotacao_ori,
    safe_cast(t.dsc_tipo_rsp as string) as tipo_rsp,
    safe_cast(t.dsc_tipo_movimentacao as string) as tipo_movimentacao,
    safe_cast(t.data_movimentacao as date) as data_movimentacao,
    safe_cast(t.valor_movimentacao as float64) as valor_movimentacao,
    safe_cast(t.dsc_documento as string) as documento,
    safe_cast(t.data_documento as date) as data_documento,
    safe_cast(t.dsc_motivo as string) as justificativa
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_restos_pagar_movimentacao_mg"
        )
    }} as t
left join p_restos_pagar on t.seq_rsp = p_restos_pagar.id_rsp
