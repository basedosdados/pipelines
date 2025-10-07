{{
    config(
        alias="mes_brasil",
        schema="br_ibge_inpc",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    case when indice = "..." then null else safe_cast(indice as float64) end as indice,
    case
        when variacao_mensal = "..."
        then null
        else safe_cast(variacao_mensal as float64)
    end as variacao_mensal,
    case
        when variacao_trimestral = "..."
        then null
        else safe_cast(variacao_trimestral as float64)
    end as variacao_trimestral,
    case
        when variacao_semestral = "..."
        then null
        else safe_cast(variacao_semestral as float64)
    end as variacao_semestral,
    case
        when variacao_anual = "..." then null else safe_cast(variacao_anual as float64)
    end as variacao_anual,
    case
        when variacao_doze_meses = "..."
        then null
        else safe_cast(variacao_doze_meses as float64)
    end as variacao_doze_meses
from
    {{ set_datalake_project("br_ibge_inpc_staging.mes_brasil") }} as t
    {# {% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %} #}
    
