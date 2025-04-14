{{
    config(
        alias="producao_pecuaria",
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
    safe_cast(ovinos_tosquiados as int64) ovinos_tosquiados,
    safe_cast(vacas_ordenhadas as int64) vacas_ordenhadas,
from {{ set_datalake_project("br_ibge_ppm_staging.producao_pecuaria") }} as t
where ovinos_tosquiados is not null or vacas_ordenhadas is not null
