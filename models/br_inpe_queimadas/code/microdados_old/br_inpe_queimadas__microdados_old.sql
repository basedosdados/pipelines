{{
    config(
        alias="microdados_old",
        schema="br_inpe_queimadas",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2025, "interval": 1},
        },
        materialized="table",
        labels={"tema": "meio-ambiente"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(bioma as string) bioma,
    safe_cast(id_bdq as string) id_bdq,
    safe_cast(id_foco as string) id_foco,
    safe_cast(data_hora as datetime) data_hora,
    st_geogpoint(
        safe_cast(longitude as float64), safe_cast(latitude as float64)
    ) centroide,
from {{ set_datalake_project("br_inpe_queimadas_staging.microdados") }} as t
