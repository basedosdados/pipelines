{{
    config(
        schema="br_mj_sisdepen",
        alias="populacao_caracteristica",
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
    safe_cast(caracteristica as string) caracteristica,
    safe_cast(categoria as string) categoria,
    safe_cast(sexo as string) sexo,
    safe_cast(quantidade as int64) quantidade,
    safe_cast(condicao_registro as string) condicao_registro
from {{ set_datalake_project("br_mj_sisdepen_staging.populacao_caracteristica") }} as t
