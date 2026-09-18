{{
    config(
        alias="relatorio",
        schema="br_bcb_ifdata",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2031, "interval": 1},
        },
        cluster_by=["mes", "id_instituicao", "id_coluna"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_instituicao as string) id_instituicao,
    safe_cast(id_coluna as string) id_coluna,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_bcb_ifdata_staging.relatorio") }} as t
