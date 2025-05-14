select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(indice as float64) indice,
    safe_cast(variacao_mensal as float64) variacao_mensal,
    safe_cast(variacao_12_meses as float64) variacao_12_meses,
    safe_cast(variacao_acumulada_ano as float64) variacao_acumulada_ano,
    safe_cast(indice_fechamento_mensal as float64) indice_fechamento_mensal
from {{ set_datalake_project("br_fgv_igp_staging.igp_10_mes") }} as t
