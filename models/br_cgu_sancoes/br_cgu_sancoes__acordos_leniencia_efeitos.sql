{{
    config(
        schema="br_cgu_sancoes",
        alias="acordos_leniencia_efeitos",
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
    safe_cast(id_acordo as string) id_acordo,
    safe_cast(efeito as string) efeito,
    safe_cast(complemento as string) complemento
from {{ set_datalake_project("br_cgu_sancoes_staging.acordos_leniencia_efeitos") }} as t
