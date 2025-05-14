select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(indice as float64) indice,
    safe_cast(variacao_mensal as float64) variacao_mensal
from {{ set_datalake_project("br_sp_saopaulo_dieese_icv_staging.mes") }} as t
