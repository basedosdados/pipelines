-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="dispensa_credenciado",
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
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_cred_dispensa` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_dispensa.id_dispensa_bd,
            ' ',
            ifnull(t.num_doc_credenciado, ''),
            ' ',
            ifnull(t.seq_cred_dispensa, '')
        ) as string
    ) as id_dispensa_credenciado_bd,
    safe_cast(p_dispensa.id_dispensa_bd as string) as id_dispensa_bd,
    safe_cast(t.seq_cred_dispensa as string) as id_cred_dispensa,
    safe_cast(t.seq_item_dispensa as string) as id_item_dispensa,
    safe_cast(t.seq_dispensa as string) as id_dispensa,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_credenciado as string) as numero_doc_credenciado,
    safe_cast(t.dsc_nom_credenciado as string) as nom_credenciado,
    safe_cast(t.data_credenciamento as date) as data_credenciamento,
    safe_cast(t.num_inscr_estadual as string) as numero_inscr_estadual,
    safe_cast(t.dsc_sigla_uf as string) as sigla_uf_credenciado,
    safe_cast(t.num_certidao_inss as string) as numero_certidao_inss,
    safe_cast(t.data_emi_cert_inss as date) as data_emi_cert_inss,
    safe_cast(t.data_val_cert_inss as date) as data_val_cert_inss,
    safe_cast(t.num_cert_fgts as string) as numero_cert_fgts,
    safe_cast(t.data_emi_cert_fgts as date) as data_emi_cert_fgts,
    safe_cast(t.data_val_cert_fgts as date) as data_val_cert_fgts,
    safe_cast(t.num_cert_negativa as string) as numero_cert_negativa,
    safe_cast(t.data_emi_cert_negativa as date) as data_emi_cert_negativa,
    safe_cast(t.data_val_cert_negativa as date) as data_val_cert_negativa
from
    {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_credenciado_mg") }}
    as t
left join p_dispensa on t.seq_dispensa = p_dispensa.seq_dispensa
