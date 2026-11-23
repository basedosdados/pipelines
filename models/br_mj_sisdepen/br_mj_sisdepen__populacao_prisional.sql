{{
    config(
        schema="br_mj_sisdepen",
        alias="populacao_prisional",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(semestre as int64) semestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_unidade as string) id_unidade,
    safe_cast(ciclo as string) ciclo,
    safe_cast(geracao_esquema as string) geracao_esquema,
    safe_cast(situacao_processual as string) situacao_processual,
    safe_cast(regime as string) regime,
    safe_cast(esfera_justica as string) esfera_justica,
    safe_cast(sexo as string) sexo,
    safe_cast(quantidade as int64) quantidade,
    safe_cast(quantidade_rdd as int64) quantidade_rdd
from {{ set_datalake_project("br_mj_sisdepen_staging.populacao_prisional") }} as t
