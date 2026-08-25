{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_remuneracao",
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
    safe_cast(mes as int64) mes,
    safe_cast(id_remuneracao as string) id_remuneracao,
    safe_cast(nome as string) nome,
    safe_cast(tipo_folha as string) tipo_folha,
    safe_cast(remuneracao_basica as float64) remuneracao_basica,
    safe_cast(vantagens_pessoais as float64) vantagens_pessoais,
    safe_cast(funcao_comissionada as float64) funcao_comissionada,
    safe_cast(gratificacao_natalina as float64) gratificacao_natalina,
    safe_cast(horas_extras as float64) horas_extras,
    safe_cast(auxilios as float64) auxilios,
    safe_cast(diarias as float64) diarias,
    safe_cast(vantagens_indenizatorias as float64) vantagens_indenizatorias,
    safe_cast(outras_eventuais as float64) outras_eventuais,
    safe_cast(abono_permanencia as float64) abono_permanencia,
    safe_cast(faltas as float64) faltas,
    safe_cast(previdencia as float64) previdencia,
    safe_cast(imposto_renda as float64) imposto_renda,
    safe_cast(reversao_teto_constitucional as float64) reversao_teto_constitucional,
    safe_cast(remuneracao_liquida as float64) remuneracao_liquida
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_remuneracao"
        )
    }} as t
