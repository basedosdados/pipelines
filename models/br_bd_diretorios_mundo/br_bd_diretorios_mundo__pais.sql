{{ config(alias="pais", schema="br_bd_diretorios_mundo", materialized="table") }}

select
    safe_cast(id_m49 as string) id_m49,
    safe_cast(replace(id_fao, ".0", "") as string) id_fao,
    safe_cast(replace(id_gaul, ".0", "") as string) id_gaul,
    safe_cast(id_cow as string) id_cow,
    safe_cast(sigla_iso3 as string) sigla_iso3,
    safe_cast(sigla_iso2 as string) sigla_iso2,
    safe_cast(sigla_pnud as string) sigla_pnud,
    safe_cast(sigla_cow as string) sigla_cow,
    safe_cast(sigla_coi as string) sigla_coi,
    safe_cast(sigla_fifa as string) sigla_fifa,
    safe_cast(nome_pt as string) nome_pt,
    safe_cast(nome_en as string) nome_en,
    safe_cast(nome_oficial_en as string) nome_oficial_en,
    safe_cast(nacionalidade as string) nacionalidade,
    safe_cast(sigla_continente as string) sigla_continente
from {{ set_datalake_project("br_bd_diretorios_mundo_staging.pais") }} as t
