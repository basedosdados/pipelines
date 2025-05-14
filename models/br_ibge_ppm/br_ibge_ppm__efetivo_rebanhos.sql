{{
    config(
        alias="efetivo_rebanhos",
        schema="br_ibge_ppm",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1974, "end": 2022, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_rebanho as string) tipo_rebanho,
    safe_cast(quantidade as int64) quantidade
from {{ set_datalake_project("br_ibge_ppm_staging.efetivo_rebanhos") }} as t
where quantidade is not null
