-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="licitacao_quadro_societario",
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
    ),
    p_licitacao_participante as (
        select distinct
            t.seq_hab_licitacao,
            concat(
                p_licitacao.id_licitacao_bd, ' ', ifnull(t.num_documento, '')
            ) as id_licitacao_participante_bd
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_participante_mg"
                )
            }} as t
        left join p_licitacao on t.seq_licitacao = p_licitacao.seq_licitacao
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_quadro_soc` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_licitacao_participante.id_licitacao_participante_bd,
            ' ',
            ifnull(t.num_documento, ''),
            ' ',
            ifnull(t.seq_quadro_soc, '')
        ) as string
    ) as id_licitacao_quadro_societario_bd,
    safe_cast(
        p_licitacao_participante.id_licitacao_participante_bd as string
    ) as id_licitacao_participante_bd,
    safe_cast(t.seq_quadro_soc as string) as id_quadro_soc,
    safe_cast(t.seq_hab_licitacao as string) as id_hab_licitacao,
    safe_cast(t.seq_licitacao as string) as id_licitacao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_documento as string) as numero_documento,
    safe_cast(t.nom_pessoa as string) as nome_pessoa,
    safe_cast(t.num_cgc_habilitado as string) as numero_cgc_habilitado,
    safe_cast(t.dsc_tipo_participacao as string) as tipo_participacao
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_licitacao_quadro_societario_mg"
        )
    }} as t
left join
    p_licitacao_participante
    on t.seq_hab_licitacao = p_licitacao_participante.seq_hab_licitacao
