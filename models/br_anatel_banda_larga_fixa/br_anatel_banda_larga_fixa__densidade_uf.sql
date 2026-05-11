{{
    config(
        alias="densidade_uf",
        schema="br_anatel_banda_larga_fixa",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(densidade as float64) densidade
from {{ set_datalake_project("br_anatel_banda_larga_fixa_staging.densidade_uf") }} as t
