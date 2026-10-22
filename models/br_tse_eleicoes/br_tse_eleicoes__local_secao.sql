-- Geolocalização das seções (Pindograma). Até 2026-10 era uma view criada fora do
-- dbt que convertia zona e secao para INT64; aqui são STRING, como no resto do
-- dataset, para que os joins com resultados e perfil do eleitorado funcionem.
{{
    config(
        schema="br_tse_eleicoes",
        alias="local_secao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2030, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(zona as string) zona,
    safe_cast(secao as string) secao,
    safe_cast(ano as int64) ano,
    st_geogpoint(
        safe_cast(
            coalesce(
                google_lon,
                tse_lon,
                comp_tse_lon,
                inep_lon,
                local_lon,
                ad_lon,
                pl_lon,
                google_approx_lon,
                ibge_approx_lon
            ) as float64
        ),
        safe_cast(
            coalesce(
                google_lat,
                tse_lat,
                comp_tse_lat,
                inep_lat,
                local_lat,
                ad_lat,
                pl_lat,
                google_approx_lat,
                ibge_approx_lat
            ) as float64
        )
    ) melhor_urbano,
    st_geogpoint(
        safe_cast(
            coalesce(
                ad_lon,
                pl_lon,
                comp_tse_lon,
                tse_lon,
                inep_lon,
                local_lon,
                google_lon,
                google_approx_lon,
                ibge_approx_lon
            ) as float64
        ),
        safe_cast(
            coalesce(
                ad_lat,
                pl_lat,
                comp_tse_lat,
                tse_lat,
                inep_lat,
                local_lat,
                google_lat,
                google_approx_lat,
                ibge_approx_lat
            ) as float64
        )
    ) melhor_rural,
    st_geogpoint(
        safe_cast(tse_lon as float64), safe_cast(tse_lat as float64)
    ) tse_recente,
    st_geogpoint(
        safe_cast(comp_tse_lon as float64), safe_cast(comp_tse_lat as float64)
    ) tse_distribuido,
    st_geogpoint(
        safe_cast(inep_lon as float64), safe_cast(inep_lat as float64)
    ) escolas_inep,
    st_geogpoint(
        safe_cast(local_lon as float64), safe_cast(local_lat as float64)
    ) escolas_municipais,
    st_geogpoint(
        safe_cast(ad_lon as float64), safe_cast(ad_lat as float64)
    ) ibge_cnefe_endereco,
    st_geogpoint(
        safe_cast(pl_lon as float64), safe_cast(pl_lat as float64)
    ) ibge_cnefe_local,
    st_geogpoint(
        safe_cast(google_lon as float64), safe_cast(google_lat as float64)
    ) google,
    st_geogpoint(
        safe_cast(places_lon as float64), safe_cast(places_lat as float64)
    ) google_relaxado,
    st_geogpoint(
        safe_cast(google_approx_lon as float64), safe_cast(google_approx_lat as float64)
    ) google_centro_geometrico,
    st_geogpoint(
        safe_cast(ibge_approx_lon as float64), safe_cast(ibge_approx_lat as float64)
    ) ibge_povoados
from {{ set_datalake_project("br_tse_eleicoes_staging.local_secao") }} as t
