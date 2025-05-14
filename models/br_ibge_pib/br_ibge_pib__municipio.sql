{{ config(alias="municipio", schema="br_ibge_pib") }}
select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(ano as int64) ano,
    1000 * safe_cast(pib as int64) pib,
    1000 * safe_cast(impostos_liquidos as int64) impostos_liquidos,
    1000 * safe_cast(va as int64) va,
    1000 * safe_cast(va_agropecuaria as int64) va_agropecuaria,
    1000 * safe_cast(va_industria as int64) va_industria,
    1000 * safe_cast(va_servicos as int64) va_servicos,
    1000 * safe_cast(va_adespss as int64) va_adespss
from {{ set_datalake_project("br_ibge_pib_staging.municipio") }} as t
