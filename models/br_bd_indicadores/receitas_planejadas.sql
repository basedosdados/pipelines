select
    safe_cast(ano_competencia as int64) ano_competencia,
    safe_cast(mes_competencia as int64) mes_competencia,
    safe_cast(ano_caixa as int64) ano_caixa,
    safe_cast(mes_caixa as int64) mes_caixa,
    safe_cast(categoria as string) categoria,
    safe_cast(tipo as string) tipo,
    safe_cast(frequencia as string) frequencia,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_bd_indicadores_staging.receitas_planejadas") }} as t
