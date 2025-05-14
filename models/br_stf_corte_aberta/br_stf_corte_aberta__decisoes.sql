{{
    config(
        schema="br_stf_corte_aberta",
        alias="decisoes",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2025, "interval": 1},
        },
        labels={"tema": "direito"},
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(classe as string) classe,
    safe_cast(numero as string) numero,
    initcap(relator) relator,
    safe_cast(link as string) link,
    initcap(subgrupo_andamento) subgrupo_andamento,
    initcap(andamento) andamento,
    initcap(observacao_andamento_decisao) observacao_andamento_decisao,
    initcap(modalidade_julgamento) modalidade_julgamento,
    initcap(tipo_julgamento) tipo_julgamento,
    initcap(meio_tramitacao) meio_tramitacao,
    safe_cast(indicador_tramitacao as bool) indicador_tramitacao,
    initcap(assunto_processo) assunto_processo,
    initcap(ramo_direito) ramo_direito,
    safe_cast(date(data_autuacao) as date) data_autuacao,
    safe_cast(date(data_decisao) as date) data_decisao,
    case
        when data_baixa_processo = '---'
        then null
        else safe_cast(data_baixa_processo as date)
    end data_baixa_processo
from {{ set_datalake_project("br_stf_corte_aberta_staging.decisoes") }} as t
