{{ config(alias="frente_deputado", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(id as string) id_frente,
    safe_cast(titulo as string) titulo_deputado,
    safe_cast(replace(id_deputado, ".0", "") as string) id_deputado,
    initcap(nome_deputado) nome_deputado,
    safe_cast(url_foto_deputado as string) url_foto_deputado,
from {{ set_datalake_project("br_camara_dados_abertos_staging.frente_deputado") }} as t
