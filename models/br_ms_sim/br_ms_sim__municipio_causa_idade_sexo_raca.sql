{{
    config(
        alias="municipio_causa_idade_sexo_raca",
        schema="br_ms_sim",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2031, "interval": 1},
        },
    )
}}

select
    ano,
    sigla_uf,
    id_municipio_residencia as id_municipio,
    causa_basica,
    cast(floor(idade) as int64) as idade,
    sexo,
    raca_cor,
    count(*) as numero_obitos
from {{ ref("br_ms_sim__microdados") }}
group by 1, 2, 3, 4, 5, 6, 7
