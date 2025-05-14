select
    safe_cast(id_pessoa as string) id_pessoa,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(equipe as string) equipe,
    safe_cast(nivel as string) nivel,
    safe_cast(cargo as string) cargo
from {{ set_datalake_project("br_bd_indicadores_staging.equipes") }} as t
