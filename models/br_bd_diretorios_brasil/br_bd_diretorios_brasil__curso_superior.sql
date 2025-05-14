{{
    config(
        alias="curso_superior",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_curso as string) id_curso,
    safe_cast(nome_curso as string) nome_curso,
    safe_cast(id_area as string) id_area,
    safe_cast(nome_area as string) nome_area,
    safe_cast(grau_academico as string) grau_academico
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.curso_superior") }} as t
