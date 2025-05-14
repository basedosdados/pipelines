select
    safe_cast(safe_cast(ano_competencia as numeric) as int64) ano_competencia,
    safe_cast(safe_cast(mes_competencia as numeric) as int64) mes_competencia,
    safe_cast(safe_cast(ano_caixa as numeric) as int64) ano_caixa,
    safe_cast(safe_cast(mes_caixa as numeric) as int64) mes_caixa,
    safe_cast(categoria as string) categoria,
    safe_cast(tipo as string) tipo,
    safe_cast(frequencia as string) frequencia,
    safe_cast(equipe as string) equipe,
    safe_cast(safe_cast(valor as numeric) as float64) valor
from {{ set_datalake_project("br_bd_indicadores_staging.contabilidade") }} as t
