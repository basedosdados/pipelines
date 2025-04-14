{{ config(alias="pais", schema="br_bd_diretorios_mundo", materialized="table") }}

select
    safe_cast(id_pais_m49 as string) id_pais_m49,
    safe_cast(replace(id_pais_fao, ".0", "") as string) id_pais_fao,
    safe_cast(replace(id_pais_gaul, ".0", "") as string) id_pais_gaul,
    safe_cast(sigla_pais_iso3 as string) sigla_pais_iso3,
    safe_cast(sigla_pais_iso2 as string) sigla_pais_iso2,
    safe_cast(sigla_pais_pnud as string) sigla_pais_pnud,
    safe_cast(sigla_pais_coi as string) sigla_pais_coi,
    safe_cast(sigla_pais_fifa as string) sigla_pais_fifa,
    safe_cast(nome as string) nome,
    safe_cast(nome_ingles as string) nome_ingles,
    safe_cast(nome_oficial_ingles as string) nome_oficial_ingles,
    safe_cast(nacionalidade as string) nacionalidade,
    safe_cast(sigla_continente as string) sigla_continente
from {{ set_datalake_project("br_bd_diretorios_mundo_staging.pais") }} as t
