{{ config(alias="domicilio_recenseado", schema="br_ibge_censo_2022") }}
with
    ibge as (
        select
            municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'([^\(]+)')) as string
            ) nome_municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'\(([^)]+)\)')) as string
            ) sigla_uf,
            safe_cast(especie as string) especie,
            safe_cast(domicilios_recenseados_domicilios_ as int64) domicilios,
        from
            {{
                set_datalake_project(
                    "br_ibge_censo_2022_staging.domicilio_recenseado"
                )
            }} as t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-staging.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
