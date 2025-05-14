{{
    config(
        schema="br_me_siconfi",
        alias="uf_despesas_funcao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_uf as string) id_uf,
    safe_cast(estagio as string) estagio,
    safe_cast(portaria as string) portaria,
    safe_cast(conta as string) conta,
    safe_cast(estagio_bd as string) estagio_bd,
    safe_cast(id_conta_bd as string) id_conta_bd,
    safe_cast(conta_bd as string) conta_bd,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_me_siconfi_staging.uf_despesas_funcao") }} as t
