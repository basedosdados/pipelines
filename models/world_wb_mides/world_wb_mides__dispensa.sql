-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="dispensa",
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
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
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
        ) as string
    ) as id_dispensa_bd,
    safe_cast(t.seq_dispensa as string) as id_dispensa,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.dsc_tipo_cadastro as string) as tipo_cadastro,
    safe_cast(t.num_processo as string) as numero_processo,
    safe_cast(t.num_ano_processo as string) as numero_ano_processo,
    safe_cast(t.dsc_tipo_processo as string) as tipo_processo,
    safe_cast(t.data_abertura as date) as data_abertura,
    safe_cast(t.dsc_nat_objeto as string) as natureza_objeto,
    safe_cast(t.dsc_objeto as string) as objeto,
    safe_cast(t.dsc_justificativa as string) as justificativa,
    safe_cast(t.dsc_razao as string) as razao,
    safe_cast(t.data_pub_termo as date) as data_pub_termo,
    safe_cast(t.dsc_veiculo_pub as string) as veiculo_publicacao,
    safe_cast(t.dsc_ind_processo_lote as string) as ind_processo_lote,
    safe_cast(t.valor_empenhado as float64) as valor_empenhado,
    safe_cast(t.valor_liquidado as float64) as valor_liquidado,
    safe_cast(t.valor_pago as float64) as valor_pago,
    safe_cast(t.valor_rsp_proc as float64) as valor_restos_pagar_processado,
    safe_cast(t.valor_rsp_nao_proc as float64) as valor_restos_pagar_nao_processado
from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_mg") }} as t
left join
    unidade_xwalk as x
    on t.id_municipio = x.id_municipio
    and t.id_unidade_gestora = x.id_unidade_gestora
