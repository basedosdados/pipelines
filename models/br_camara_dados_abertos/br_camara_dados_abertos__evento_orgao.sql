{{ config(alias="evento_orgao", schema="br_camara_dados_abertos") }}
select
    safe_cast(idevento as string) id_evento,
    safe_cast(idorgao as string) id_orgao,
    safe_cast(siglaorgao as string) sigla_orgao
from {{ project_path("br_camara_dados_abertos_staging.evento_orgao") }} as t
