{{
    config(
        alias="municipio",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_6 as string) id_municipio_6,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(id_municipio_rf as string) id_municipio_rf,
    safe_cast(id_municipio_bcb as string) id_municipio_bcb,
    safe_cast(nome as string) nome,
    safe_cast(capital_uf as int64) capital_uf,
    safe_cast(id_comarca as string) id_comarca,
    safe_cast(id_regiao_saude as string) id_regiao_saude,
    safe_cast(nome_regiao_saude as string) nome_regiao_saude,
    safe_cast(id_regiao_imediata as string) id_regiao_imediata,
    safe_cast(nome_regiao_imediata as string) nome_regiao_imediata,
    safe_cast(id_regiao_intermediaria as string) id_regiao_intermediaria,
    safe_cast(nome_regiao_intermediaria as string) nome_regiao_intermediaria,
    safe_cast(id_microrregiao as string) id_microrregiao,
    safe_cast(nome_microrregiao as string) nome_microrregiao,
    safe_cast(id_mesorregiao as string) id_mesorregiao,
    safe_cast(nome_mesorregiao as string) nome_mesorregiao,
    safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
    safe_cast(nome_regiao_metropolitana as string) nome_regiao_metropolitana,
    safe_cast(ddd as string) ddd,
    safe_cast(id_uf as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(nome_uf as string) nome_uf,
    safe_cast(nome_regiao as string) nome_regiao,
    safe_cast(amazonia_legal as int64) amazonia_legal,
    safe.st_geogfromtext(centroide) centroide
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.municipio") }} as t
