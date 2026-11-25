-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="nota_fiscal",
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
            ifnull(t.num_doc_emitente, ''),
            ' ',
            ifnull(t.num_nota_fiscal, ''),
            ' ',
            ifnull(t.num_serie_nota_fiscal, ''),
            ' ',
            ifnull(t.data_emissao, ''),
            ' ',
            ifnull(t.cod_chave_nota_fis, ''),
            ' ',
            t.id_municipio,
            ' ',
            t.ano
        ) as string
    ) as id_nota_fiscal_bd,
    safe_cast(t.seq_nota_fiscal as string) as id_nota_fiscal,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_emitente as string) as numero_doc_emitente,
    safe_cast(t.nom_emitente as string) as nome_emitente,
    safe_cast(t.num_nota_fiscal as string) as numero_nota_fiscal,
    safe_cast(t.num_serie_nota_fiscal as string) as numero_serie_nota_fiscal,
    safe_cast(t.num_inscr_est_emitente as string) as numero_inscr_est_emitente,
    safe_cast(t.num_inscr_mun_emitente as string) as numero_inscr_mun_emitente,
    safe_cast(t.dsc_mun_emitente as string) as mun_emitente,
    safe_cast(t.cod_cep_mun_emitente as string) as codigo_cep_mun_emitente,
    safe_cast(t.cod_est_credor as string) as codigo_est_credor,
    safe_cast(t.dsc_tipo_nota_fiscal as string) as tipo_nota_fiscal,
    safe_cast(t.cod_chave_nota_fis as string) as codigo_chave_nota_fis,
    safe_cast(t.cod_chave_mun_nota_fis as string) as codigo_chave_mun_nota_fis,
    safe_cast(t.num_aut_impressao as string) as numero_aut_impressao,
    safe_cast(t.data_emissao as date) as data_emissao,
    safe_cast(t.data_vencimento as date) as data_vencimento,
    safe_cast(t.valor_bruto as float64) as valor_bruto,
    safe_cast(t.valor_desconto as float64) as valor_desconto,
    safe_cast(t.valor_liquido as float64) as valor_liquido
from {{ set_datalake_project("world_wb_mides_staging.raw_nota_fiscal_mg") }} as t
