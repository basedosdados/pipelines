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

-- Base dos agregados de br_ms_sim: as demais tabelas somam a partir desta.
with
    obitos as (
        select
            ano,
            -- 1996-1998: a capital do RJ vem dividida em códigos 3345xxx.
            if(
                id_municipio_residencia like '3345%', '3304557', id_municipio_residencia
            ) as id_municipio,
            causa_basica,
            cast(floor(idade) as int64) as idade,
            sexo,
            raca_cor
        from {{ ref("br_ms_sim__microdados") }}
        -- XX00000 é município ignorado, só com a UF.
        where
            id_municipio_residencia is not null
            and id_municipio_residencia not like '__00000'
    )

-- A UF vem do código do município, não do arquivo de origem.
select
    o.ano,
    uf.sigla as sigla_uf,
    o.id_municipio,
    o.causa_basica,
    o.idade,
    o.sexo,
    o.raca_cor,
    count(*) as numero_obitos
from obitos as o
left join
    basedosdados.br_bd_diretorios_brasil.uf as uf on uf.id_uf = left(o.id_municipio, 2)
group by 1, 2, 3, 4, 5, 6, 7
