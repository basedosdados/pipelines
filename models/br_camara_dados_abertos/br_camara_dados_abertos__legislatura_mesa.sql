{{ config(alias="legislatura_mesa", schema="br_camara_dados_abertos") }}
select
    safe_cast(idlegislatura as string) id_legislatura,
    safe_cast(datainicio as datetime) data_inicio,
    safe_cast(datafim as datetime) data_final,
    safe_cast(idorgao as string) id_orgao,
    safe_cast(uriorgao as string) url_orgao,
    safe_cast(siglaorgao as string) sigla_orgao,
    safe_cast(nomeorgao as string) nome_orgao,
    safe_cast(iddeputado as string) id_deputado,
    safe_cast(nomedeputado as string) nome_deputado,
    safe_cast(cargo as string) cargo,
    safe_cast(urideputado as string) url_deputado,
    safe_cast(siglapartido as string) sigla_partido,
    safe_cast(siglauf as string) sigla_uf,
from {{ set_datalake_project("br_camara_dados_abertos_staging.legislatura_mesa") }} as t
