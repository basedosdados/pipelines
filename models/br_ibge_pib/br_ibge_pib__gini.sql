{{ config(alias="gini", schema="br_ibge_pib") }}
select
    safe_cast(cod as string) id_uf,
    safe_cast(ano as int64) ano,
    safe_cast(replace(gini_pib, ",", ".") as float64) gini_pib,
    safe_cast(replace(gini_va_agro, ",", ".") as float64) gini_va_agro,
    safe_cast(replace(gini_va_industria, ",", ".") as float64) gini_va_industria,
    safe_cast(replace(gini_servicos, ",", ".") as float64) gini_va_servicos,
    safe_cast(replace(gini_va_adespss, ",", ".") as float64) gini_va_adespss,
from {{ set_datalake_project("br_ibge_pib_staging.gini") }} as t
