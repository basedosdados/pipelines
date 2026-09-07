{{
    config(
        schema="br_fnde_fundeb",
        alias="indicador_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(bimestre as int64) bimestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_indicador as string) id_indicador,
    safe_cast(codigo_indicador as string) codigo_indicador,
    safe_cast(valor_percentual as float64) valor_percentual,
    safe_cast(valor_real as float64) valor_real
from {{ set_datalake_project("br_fnde_fundeb_staging.indicador_estadual") }} as t
