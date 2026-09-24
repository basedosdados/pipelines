-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato",
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
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_contrato` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    -- A residual handful of source rows repeat the key above; `seq_dispensa`
    -- separates them. See the note on the other seq-bearing keys.
    safe_cast(
        concat(
            t.orgao,
            ' ',
            ifnull(t.cod_unidade, ''),
            ' ',
            ifnull(t.cod_subunidade, ''),
            ' ',
            ifnull(t.num_contrato, ''),
            ' ',
            ifnull(t.num_ano_contrato, ''),
            ' ',
            t.id_municipio,
            ' ',
            t.ano,
            ' ',
            ifnull(t.seq_contrato, ''),
            ' ',
            ifnull(t.seq_dispensa, '')
        ) as string
    ) as id_contrato_bd,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.cod_unidade as string) as codigo_unidade,
    safe_cast(t.cod_subunidade as string) as codigo_subunidade,
    safe_cast(t.seq_licitacao as string) as id_licitacao,
    safe_cast(t.seq_dispensa as string) as id_dispensa,
    safe_cast(t.dsc_decorrentelicitacao as string) as decorrentelicitacao,
    safe_cast(t.dsc_tipo_processo as string) as tipo_processo,
    safe_cast(t.dsc_naturezaobjeto as string) as naturezaobjeto,
    safe_cast(t.dsc_tipo_cadastro as string) as tipo_cadastro,
    safe_cast(t.num_contrato as string) as numero_contrato,
    safe_cast(t.data_assinatura as date) as data_assinatura,
    safe_cast(t.num_ano_contrato as string) as numero_ano_contrato,
    safe_cast(t.dsc_objetocontrato as string) as objetocontrato,
    safe_cast(t.num_doc_signatario as string) as numero_doc_signatario,
    safe_cast(t.dsc_nome_signatario as string) as nome_signatario,
    safe_cast(t.tipo_instrumento as string) as tipo_instrumento,
    safe_cast(t.data_iniciovigencia as date) as data_iniciovigencia,
    safe_cast(t.data_fimvigencia as date) as data_fimvigencia,
    safe_cast(t.dsc_fornecimento as string) as fornecimento,
    safe_cast(t.dsc_formapagamento as string) as formapagamento,
    safe_cast(t.dsc_unid_med_prazo as string) as unid_med_prazo,
    safe_cast(t.dsc_prazoexecucao as string) as prazoexecucao,
    safe_cast(t.dsc_multarecisoria as string) as multarecisoria,
    safe_cast(t.dsc_multa_inadimplencia as string) as multa_inadimplencia,
    safe_cast(t.dsc_garantia as string) as garantia,
    safe_cast(t.data_publicacao as date) as data_publicacao,
    safe_cast(t.dsc_veiculopublicacao as string) as veiculopublicacao,
    safe_cast(t.valor_contrato as float64) as valor_contrato,
    safe_cast(t.valor_empenhado as float64) as valor_empenhado,
    safe_cast(t.valor_liquidado as float64) as valor_liquidado,
    safe_cast(t.valor_pago as float64) as valor_pago,
    safe_cast(t.valor_rspprocessado as float64) as valor_rspprocessado,
    safe_cast(t.valor_rspnprocessado as float64) as valor_rspnprocessado
from {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }} as t
