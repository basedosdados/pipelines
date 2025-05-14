{{
    config(
        alias="setor_censitario_2022",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

with
    tb as (
        select
            safe_cast(cd_setor as string) as id_setor_censitario,
            safe_cast(cd_regiao as string) as id_regiao,
            safe_cast(nm_regiao as string) as nome_regiao,
            safe_cast(cd_uf as string) as id_uf,
            safe_cast(cd_mun as string) as id_municipio,
            safe_cast(cd_dist as string) as id_distrito,
            safe_cast(nm_dist as string) as nome_distrito,
            safe_cast(cd_subdist as string) as id_subdistrito,
            safe_cast(nm_subdist as string) as nome_subdistrito,
            safe_cast(cd_micro as string) as id_microrregiao,
            safe_cast(cd_meso as string) as id_mesorregiao,
            safe_cast(cd_rgi as string) as id_regiao_imediata,
            safe_cast(cd_rgint as string) as id_regiao_intermediaria,
            safe_cast(cd_concurb as string) as id_concentracao_urbana,
            safe_cast(nm_concurb as string) as nome_concentracao_urbana,
            safe_cast(area_km2 as float64) as area_km2,
        from
            {{
                set_datalake_project(
                    "br_ibge_censo_2022_staging.domicilio_morador_setor_censitario"
                )
            }}
    )

select
    id_setor_censitario,
    id_regiao,
    a.nome_regiao,
    a.id_uf,
    b.nome as nome_uf,
    a.id_municipio,
    c.nome as nome_municipio,
    a.id_distrito,
    a.nome_distrito,
    id_subdistrito,
    nome_subdistrito,
    a.id_microrregiao,
    c.nome_microrregiao,
    a.id_mesorregiao,
    c.nome_mesorregiao,
    a.id_regiao_imediata,
    c.nome_regiao_imediata,
    a.id_regiao_intermediaria,
    c.nome_regiao_intermediaria,
    a.id_concentracao_urbana,
    a.nome_concentracao_urbana,
    a.area_km2,
from tb as a
left join `basedosdados.br_bd_diretorios_brasil.uf` as b on a.id_uf = b.id_uf
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as c
    on a.id_municipio = c.id_municipio
