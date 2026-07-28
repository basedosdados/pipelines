{{
    config(
        schema="br_me_siconfi",
        alias="uf_variacoes_patrimoniais",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2025, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(portaria as string) portaria,
    safe_cast(conta as string) conta,
    safe_cast(id_conta_bd as string) id_conta_bd,
    safe_cast(conta_bd as string) conta_bd,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_me_siconfi_staging.uf_variacoes_patrimoniais") }} as t
