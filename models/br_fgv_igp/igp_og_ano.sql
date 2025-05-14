select
    safe_cast(ano as int64) ano,
    safe_cast(indice_medio as float64) indice_medio,
    safe_cast(indice as float64) indice,
    safe_cast(variacao_anual as float64) variacao_anual,
    safe_cast(indice_fechamento_anual as float64) indice_fechamento_anual
from {{ set_datalake_project("br_fgv_igp_staging.igp_og_ano") }} as t
