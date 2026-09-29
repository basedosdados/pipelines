-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="restos_pagar",
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
select
    -- `raw_rsp_mg` carries no `ano`/`mes`: the exercise is num_ano_referencia.
    safe_cast(t.num_ano_referencia as int64) as ano,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            t.orgao,
            ' ',
            ifnull(t.numero_empenho, ''),
            ' ',
            ifnull(t.num_ano_emp_origem, ''),
            ' ',
            ifnull(t.data_origem, ''),
            ' ',
            ifnull(t.dsc_dotacao_ori, ''),
            ' ',
            t.id_municipio,
            ' ',
            t.num_ano_referencia
        ) as string
    ) as id_restos_pagar_bd,
    safe_cast(t.id_rsp as string) as id_rsp,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.num_ano_referencia as string) as numero_ano_referencia,
    safe_cast(t.num_mes_referencia as string) as numero_mes_referencia,
    safe_cast(t.id_empenho_origem as string) as id_empenho_origem,
    safe_cast(t.numero_empenho as string) as numero_empenho,
    safe_cast(t.data_origem as date) as data_origem,
    safe_cast(t.num_ano_emp_origem as string) as numero_ano_emp_origem,
    safe_cast(t.dsc_dotacao_ori as string) as dotacao_ori,
    safe_cast(t.valor_original as float64) as valor_original,
    safe_cast(t.valor_processado as float64) as valor_processado,
    safe_cast(t.valor_nao_processado as float64) as valor_nao_processado,
    safe_cast(t.num_versao_arq as string) as numero_versao_arq
from {{ set_datalake_project("world_wb_mides_staging.raw_rsp_mg") }} as t
