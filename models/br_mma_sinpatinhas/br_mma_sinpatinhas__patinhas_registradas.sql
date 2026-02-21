{{
    config(
        alias="patinhas_registradas",
        schema="br_mma_sinpatinhas",
        materialized="table",
    )
}}
select
    safe_cast(data_cadastro as datetime) data_cadastro,
    safe_cast(id_animal as string) id_animal,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(especie as string) especie,
    safe_cast(idade as int64) idade,
    safe_cast(sexo as string) sexo,
    safe_cast(cor_pelagem as string) cor_pelagem,
from {{ set_datalake_project("br_mma_sinpatinhas_staging.patinhas_registradas") }} as t
