{{
    config(
        alias="uf",
        schema="br_mme_consumo_energia_eletrica",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(tipo_consumo as string) as tipo_consumo,
    case
        when numero_consumidores = '0'
        then null
        else safe_cast(numero_consumidores as int64)
    end as numero_consumidores,
    safe_cast(consumo as int64) as consumo
from {{ set_datalake_project("br_mme_consumo_energia_eletrica_staging.uf") }} as t
