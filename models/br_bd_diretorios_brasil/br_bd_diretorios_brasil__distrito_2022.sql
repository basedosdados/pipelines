{{
    config(
        alias="distrito_2022",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select distinct
    safe_cast(cd_dist as string) as id_distrito,
    safe_cast(nm_dist as string) as nome_distrito,
    safe_cast(cd_mun as string) as id_municipio,
    safe_cast(sigla as string) as sigla_uf,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.domicilio_morador_setor_censitario"
        )
    }} as a
left join `basedosdados.br_bd_diretorios_brasil.uf` as b on a.id_uf = b.id_uf
