-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="licitacao_responsavel",
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
    p_licitacao as (
        select distinct
            t.seq_licitacao,
            concat(
                t.orgao,
                ' ',
                ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                ' ',
                ifnull(t.num_processo, ''),
                ' ',
                ifnull(t.num_ano_processo, ''),
                ' ',
                ifnull(t.data_abert_proc_adm, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_licitacao_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }} as t
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
            p_licitacao.id_licitacao_bd,
            ' ',
            ifnull(t.num_documento, ''),
            ' ',
            ifnull(t.dsc_tipo_resp, '')
        ) as string
    ) as id_licitacao_responsavel_bd,
    safe_cast(p_licitacao.id_licitacao_bd as string) as id_licitacao_bd,
    safe_cast(t.seq_resp_licitacao as string) as id_resp_licitacao,
    safe_cast(t.seq_licitacao as string) as id_licitacao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_tipo_resp as string) as tipo_responsavel,
    safe_cast(t.num_documento as string) as numero_documento,
    safe_cast(t.nom_responsavel as string) as nome_responsavel
from
    {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_responsavel_mg") }}
    as t
left join p_licitacao on t.seq_licitacao = p_licitacao.seq_licitacao
