{{ config(alias="face_quadra", schema="br_ce_fortaleza_sefin_iptu") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_face_quadra as string) id_face_quadra,
    replace(logradouro, "nan", "") logradouro,
    safe_cast(metrica as string) metrica,
    safe_cast(pavimentacao as string) pavimentacao,
    safe_cast(indicador_agua as bool) indicador_agua,
    safe_cast(indicador_esgoto as bool) indicador_esgoto,
    safe_cast(indicador_galeria_pluvial as bool) indicador_galeria_pluvial,
    safe_cast(indicador_sarjeta as bool) indicador_sarjeta,
    safe_cast(indicador_iluminacao_publica as bool) indicador_iluminacao_publica,
    safe_cast(indicador_arborizacao as bool) indicador_arborizacao,
    safe.st_geogfromtext(geometria) centroide,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_ce_fortaleza_sefin_iptu_staging.face_quadra") }} as t
