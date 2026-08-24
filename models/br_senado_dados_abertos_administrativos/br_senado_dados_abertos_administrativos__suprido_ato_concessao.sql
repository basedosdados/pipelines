{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="suprido_ato_concessao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(codigo_ato_concessao as string) codigo_ato_concessao,
    safe_cast(codigo_suprido as string) codigo_suprido,
    safe_cast(nome_suprido as string) nome_suprido,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(sigla_orgao as string) sigla_orgao,
    safe_cast(orgao as string) orgao,
    safe_cast(matricula_solicitante as string) matricula_solicitante,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(indicador_regime_especial as string) indicador_regime_especial,
    safe_cast(data as date) data,
    safe_cast(data_publicacao_basf as date) data_publicacao_basf,
    safe_cast(prazo_aplicacao as date) prazo_aplicacao,
    safe_cast(prazo_comprovacao as date) prazo_comprovacao,
    safe_cast(data_aplicacao as date) data_aplicacao,
    safe_cast(data_comprovacao as date) data_comprovacao,
    safe_cast(valor_total_elementos_despesa as float64) valor_total_elementos_despesa,
    safe_cast(valor_total_empenhos as float64) valor_total_empenhos,
    safe_cast(valor_total_transacoes as float64) valor_total_transacoes,
    safe_cast(valor_total_movimentacoes as float64) valor_total_movimentacoes,
    safe_cast(data_processamento as datetime) data_processamento
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.suprido_ato_concessao"
        )
    }} as t
