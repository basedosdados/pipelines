{{
    config(
        alias="coluna",
        schema="br_bcb_ifdata",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2031, "interval": 1},
        },
        cluster_by=["mes", "id_relatorio"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_relatorio as string) id_relatorio,
    safe_cast(id_coluna as string) id_coluna,
    safe_cast(tipo_consolidado as string) tipo_consolidado,
    safe_cast(nome_relatorio as string) nome_relatorio,
    safe_cast(nome_grupo as string) nome_grupo,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(nome_coluna_ingles as string) nome_coluna_ingles,
    safe_cast(ordem_coluna as int64) ordem_coluna
from {{ set_datalake_project("br_bcb_ifdata_staging.coluna") }} as t
