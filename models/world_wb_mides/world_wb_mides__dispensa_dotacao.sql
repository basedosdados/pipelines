-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="dispensa_dotacao",
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
    -- seq_unidade -> cod_unidade. The procurement streams publish only the
    -- sequence form of the managing unit, and that is exactly what distinguishes
    -- rows sharing a process number. The spend streams publish both, so the code
    -- is recoverable. Measured: 9,319 pairs, 0 conflicts, 98.3% coverage.
    unidade_xwalk as (
        select distinct id_municipio, id_unidade_gestora, cod_unidade
        from
            (
                select id_municipio, id_unidade_gestora, cod_unidade
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_dotacao_mg"
                        )
                    }}
            )
    ),
    p_dispensa as (
        select distinct
            t.seq_dispensa,
            concat(
                t.orgao,
                ' ',
                ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                ' ',
                ifnull(t.num_processo, ''),
                ' ',
                ifnull(t.num_ano_processo, ''),
                ' ',
                ifnull(t.data_abertura, ''),
                ' ',
                ifnull(t.dsc_tipo_processo, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_dispensa_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_dispensa.id_dispensa_bd,
            ' ',
            ifnull(t.dsc_dotacao, ''),
            ' ',
            ifnull(t.dsc_fonte_recurso, '')
        ) as string
    ) as id_dispensa_dotacao_bd,
    safe_cast(p_dispensa.id_dispensa_bd as string) as id_dispensa_bd,
    safe_cast(t.seq_rec_dispensa as string) as id_rec_dispensa,
    safe_cast(t.seq_dispensa as string) as id_dispensa,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_tipo_processo as string) as tipo_processo,
    safe_cast(t.dsc_dotacao as string) as dotacao,
    safe_cast(t.dsc_funcao as string) as funcao,
    safe_cast(t.dsc_subfuncao as string) as subfuncao,
    safe_cast(t.dsc_programa as string) as programa,
    safe_cast(t.dsc_acao as string) as acao,
    safe_cast(t.dsc_subacao as string) as subacao,
    safe_cast(t.dsc_nat_despesa as string) as natureza_despesa,
    safe_cast(t.dsc_fonte_recurso as string) as fonte_recurso,
    safe_cast(t.valor_recurso as float64) as valor_recurso
from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_dotacao_mg") }} as t
left join p_dispensa on t.seq_dispensa = p_dispensa.seq_dispensa
