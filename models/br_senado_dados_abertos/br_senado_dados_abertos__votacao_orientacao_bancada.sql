{{
    config(
        alias="votacao_orientacao_bancada",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_votacao_sve as string) id_votacao_sve,
    safe_cast(sequencial_votacao as string) sequencial_votacao,
    safe_cast(data_votacao as date) data_votacao,
    safe_cast(sigla_materia as string) sigla_materia,
    safe_cast(numero_materia as string) numero_materia,
    safe_cast(ano_materia as int64) ano_materia,
    safe_cast(bancada as string) bancada,
    safe_cast(orientacao as string) orientacao,
    safe_cast(data_hora_orientacao as datetime) data_hora_orientacao,
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_staging.votacao_orientacao_bancada"
        )
    }} as t
