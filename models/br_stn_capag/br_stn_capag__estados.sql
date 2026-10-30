{{
    config(
        schema="br_stn_capag",
        alias="estados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2030, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(data_referencia as date) data_referencia,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(indicador_endividamento as float64) indicador_endividamento,
    safe_cast(nota_endividamento as string) nota_endividamento,
    safe_cast(indicador_poupanca_corrente as float64) indicador_poupanca_corrente,
    safe_cast(nota_poupanca_corrente as string) nota_poupanca_corrente,
    safe_cast(indicador_liquidez as float64) indicador_liquidez,
    safe_cast(nota_liquidez as string) nota_liquidez,
    safe_cast(nota_capag as string) nota_capag,
from {{ set_datalake_project("br_stn_capag_staging.estados") }} as t
