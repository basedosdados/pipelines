-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="despesa_dotacao",
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
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            t.orgao,
            ' ',
            ifnull(t.cod_unidade, ''),
            ' ',
            ifnull(t.cod_subunidade, ''),
            ' ',
            ifnull(t.mes, ''),
            ' ',
            ifnull(t.dsc_cod_orcamentario, ''),
            ' ',
            ifnull(t.dsc_funcao, ''),
            ' ',
            ifnull(t.dsc_subfuncao, ''),
            ' ',
            ifnull(t.dsc_programa, ''),
            ' ',
            ifnull(t.dsc_acao, ''),
            ' ',
            ifnull(t.dsc_subacao, ''),
            ' ',
            ifnull(t.dsc_naturezadespesa, ''),
            ' ',
            ifnull(t.dsc_fonterecurso, ''),
            ' ',
            t.id_municipio,
            ' ',
            t.ano
        ) as string
    ) as id_despesa_dotacao_bd,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.cod_unidade as string) as codigo_unidade,
    safe_cast(t.cod_subunidade as string) as codigo_subunidade,
    safe_cast(t.dsc_funcao as string) as funcao,
    safe_cast(t.dsc_subfuncao as string) as subfuncao,
    safe_cast(t.dsc_programa as string) as programa,
    safe_cast(t.dsc_acao as string) as acao,
    safe_cast(t.dsc_subacao as string) as subacao,
    safe_cast(t.dsc_naturezadespesa as string) as naturezadespesa,
    safe_cast(t.dsc_fonterecurso as string) as fonterecurso,
    safe_cast(t.dsc_cod_orcamentario as string) as cod_orcamentario,
    safe_cast(t.valor_previsto as float64) as valor_previsto,
    safe_cast(t.valor_acrescimo as float64) as valor_acrescimo,
    safe_cast(t.valor_deducao as float64) as valor_deducao,
    safe_cast(t.valor_empenhado as float64) as valor_empenhado,
    safe_cast(t.valor_liquidado as float64) as valor_liquidado,
    safe_cast(t.valor_pago as float64) as valor_pago,
    safe_cast(t.valor_rspprocessado as float64) as valor_rspprocessado,
    safe_cast(t.valor_rspnprocessado as float64) as valor_rspnprocessado
from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_dotacao_mg") }} as t
