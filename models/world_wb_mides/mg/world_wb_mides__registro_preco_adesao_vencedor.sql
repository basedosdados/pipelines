-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="registro_preco_adesao_vencedor",
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
    p_registro_preco_adesao as (
        select distinct
            t.seq_reg_adesao,
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
            ) as id_registro_preco_adesao_bd
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_registro_preco_adesao_mg"
                )
            }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
    ),
    p_registro_preco_adesao_item as (
        select distinct
            t.seq_item_reg_adesao,
            concat(
                p_registro_preco_adesao.id_registro_preco_adesao_bd,
                ' ',
                ifnull(t.num_lote, ''),
                ' ',
                ifnull(t.num_item, '')
            ) as id_registro_preco_adesao_item_bd
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_registro_preco_adesao_item_mg"
                )
            }} as t
        left join
            p_registro_preco_adesao
            on t.seq_reg_adesao = p_registro_preco_adesao.seq_reg_adesao
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_venc_reg_adesao` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd,
            ' ',
            ifnull(t.num_doc_vencedor, ''),
            ' ',
            ifnull(t.seq_venc_reg_adesao, '')
        ) as string
    ) as id_registro_preco_adesao_vencedor_bd,
    safe_cast(
        p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd as string
    ) as id_registro_preco_adesao_item_bd,
    safe_cast(t.seq_venc_reg_adesao as string) as id_venc_reg_adesao,
    safe_cast(t.seq_item_reg_adesao as string) as id_item_reg_adesao,
    safe_cast(t.seq_reg_adesao as string) as id_reg_adesao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_vencedor as string) as numero_doc_vencedor,
    safe_cast(t.nom_vencedor as string) as nome_vencedor,
    safe_cast(t.valor_preco_unitario as float64) as valor_preco_unitario,
    safe_cast(t.num_quant_licitado as string) as numero_quant_licitado,
    safe_cast(t.num_quant_aderido as string) as numero_quant_aderido,
    safe_cast(t.valor_pct_desconto as float64) as valor_pct_desconto
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_registro_preco_adesao_vencedor_mg"
        )
    }} as t
left join
    p_registro_preco_adesao_item
    on t.seq_item_reg_adesao = p_registro_preco_adesao_item.seq_item_reg_adesao
