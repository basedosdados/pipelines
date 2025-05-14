{{ config(alias="violencia_escola", schema="br_fbsp_absp") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(uf as string) uf,
    safe_cast(tema as string) tema,
    safe_cast(item as string) item,
    safe_cast(quantidade_escola as float64) quantidade_escola
from {{ set_datalake_project("br_fbsp_absp_staging.violencia_escola") }} as t
