-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="registro_preco_adesao",
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
            t.id_municipio,
            ' ',
            t.ano
        ) as string
    ) as id_registro_preco_adesao_bd,
    safe_cast(t.seq_reg_adesao as string) as id_reg_adesao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.seq_licitacao as string) as id_licitacao,
    safe_cast(t.dsc_tipo_cadastro as string) as tipo_cadastro,
    safe_cast(t.num_processo as string) as numero_processo,
    safe_cast(t.num_ano_processo as string) as numero_ano_processo,
    safe_cast(t.data_abertura as date) as data_abertura,
    safe_cast(t.dsc_org_gerenciador as string) as org_gerenciador,
    safe_cast(t.dsc_modalidade as string) as modalidade,
    safe_cast(t.num_modalidade as string) as numero_modalidade,
    safe_cast(t.data_ata_reg_preco as date) as data_ata_reg_preco,
    safe_cast(t.data_validade as date) as data_validade,
    safe_cast(t.dsc_nat_processo as string) as nat_processo,
    safe_cast(t.data_pub_aviso_inst as date) as data_pub_aviso_inst,
    safe_cast(t.dsc_objeto_adesao as string) as objeto_adesao,
    safe_cast(t.num_doc_resp as string) as numero_doc_responsavel,
    safe_cast(t.nom_pessoa_resp as string) as nome_pessoa_resp,
    safe_cast(t.dsc_ind_desc_tab_preco as string) as ind_desc_tab_preco,
    safe_cast(t.dsc_ind_processo_lote as string) as ind_processo_lote,
    safe_cast(t.valor_empenhado as float64) as valor_empenhado,
    safe_cast(t.valor_liquidado as float64) as valor_liquidado,
    safe_cast(t.valor_pago as float64) as valor_pago,
    safe_cast(t.valor_rsp_processado as float64) as valor_restos_pagar_processado,
    safe_cast(
        t.valor_rsp_nao_processado as float64
    ) as valor_restos_pagar_nao_processado
from
    {{ set_datalake_project("world_wb_mides_staging.raw_registro_preco_adesao_mg") }}
    as t
left join
    unidade_xwalk as x
    on t.id_municipio = x.id_municipio
    and t.id_unidade_gestora = x.id_unidade_gestora
