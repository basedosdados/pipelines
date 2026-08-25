{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador_aposentado_pensionista",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(nome as string) nome,
    safe_cast(tipo_beneficio as string) tipo_beneficio,
    safe_cast(regime as string) regime,
    safe_cast(tipo as string) tipo,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(valor_remuneracao as float64) valor_remuneracao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador_aposentado_pensionista"
        )
    }}
    as t
