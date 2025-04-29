{{ config(alias="evento_requerimento", schema="br_camara_dados_abertos") }}
select
    safe_cast(idevento as string) id_evento,
    safe_cast(titulorequerimento as string) titulo_requerimento,
    regexp_extract(urirequerimento, r'/([^/]+)$') id_proposicao
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.evento_requerimento") }}
    as t
