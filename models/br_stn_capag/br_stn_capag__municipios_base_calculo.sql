{{
    config(
        schema="br_stn_capag",
        alias="municipios_base_calculo",
        materialized="table",
        partition_by={
            "field": "ano_base",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2030, "interval": 1},
        },
        cluster_by=["id_municipio"],
    )
}}

select
    safe_cast(ano_base as int64) ano_base,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(demonstrativo as string) demonstrativo,
    safe_cast(tipo as string) tipo,
    safe_cast(codigo_conta as string) codigo_conta,
    safe_cast(conta as string) conta,
    safe_cast(detalhe as string) detalhe,
    safe_cast(valor as float64) valor,
    safe_cast(data_referencia as date) data_referencia,
from {{ set_datalake_project("br_stn_capag_staging.municipios_base_calculo") }} as t
