{{
    config(
        alias="area_conhecimento",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select distinct
    safe_cast(especialidade as string) especialidade,
    safe_cast(descricao_especialidade as string) descricao_especialidade,
    safe_cast(subarea as string) subarea,
    safe_cast(descricao_subarea as string) descricao_subarea,
    safe_cast(area as string) area,
    safe_cast(descricao_area as string) descricao_area,
    safe_cast(grande_area as string) grande_area,
    safe_cast(descricao_grande_area as string) descricao_grande_area
from
    {{ set_datalake_project("br_bd_diretorios_brasil_staging.area_conhecimento") }} as t
