{{ config(alias="escola", schema="br_inep_indicador_nivel_socioeconomico") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(area as string) area,
    safe_cast(rede as string) rede,
    safe_cast(id_escola as string) id_escola,
    safe_cast(tipo_localizacao as string) tipo_localizacao,
    safe_cast(quantidade_alunos_inse as int64) quantidade_alunos_inse,
    safe_cast(inse as float64) inse,
    safe_cast(percentual_nivel_1 as float64) percentual_nivel_1,
    safe_cast(percentual_nivel_2 as float64) percentual_nivel_2,
    safe_cast(percentual_nivel_3 as float64) percentual_nivel_3,
    safe_cast(percentual_nivel_4 as float64) percentual_nivel_4,
    safe_cast(percentual_nivel_5 as float64) percentual_nivel_5,
    safe_cast(percentual_nivel_6 as float64) percentual_nivel_6,
    safe_cast(percentual_nivel_7 as float64) percentual_nivel_7,
    safe_cast(percentual_nivel_8 as float64) percentual_nivel_8,
    safe_cast(classificacao as string) classificacao
from {{ set_datalake_project("br_inep_indicador_nivel_socioeconomico_staging.escola") }}
