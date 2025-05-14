{{
    config(
        alias="producao_aquicultura",
        schema="br_ibge_ppm",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2022, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(produto as string) produto,
    safe_cast(quantidade as int64) quantidade,
    safe_cast(valor as int64) valor,
from {{ set_datalake_project("br_ibge_ppm_staging.producao_aquicultura") }} as t
where quantidade is not null
