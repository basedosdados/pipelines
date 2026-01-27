{{
    config(
        alias="uf_remuneracao_docentes",
        schema="br_inep_indicadores_educacionais",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(rede as string) rede,
    safe_cast(escolaridade as string) escolaridade,
    safe_cast(numero_docentes as int64) numero_docentes,
    safe_cast(prop_docentes_rais as float64) prop_docentes_rais,
    safe_cast(rem_bruta_rais_1_quartil as float64) rem_bruta_rais_1_quartil,
    safe_cast(rem_bruta_rais_mediana as float64) rem_bruta_rais_mediana,
    safe_cast(rem_bruta_rais_media as float64) rem_bruta_rais_media,
    safe_cast(rem_bruta_rais_3_quartil as float64) rem_bruta_rais_3_quartil,
    safe_cast(rem_bruta_rais_desvio_padrao as float64) rem_bruta_rais_desvio_padrao,
    safe_cast(carga_horaria_media_semanal as float64) carga_horaria_media_semanal,
    safe_cast(rem_media_40_horas_semanais as float64) rem_media_40_horas_semanais,
from
    {{
        set_datalake_project(
            "br_inep_indicadores_educacionais_staging.uf_remuneracao_docentes"
        )
    }}
