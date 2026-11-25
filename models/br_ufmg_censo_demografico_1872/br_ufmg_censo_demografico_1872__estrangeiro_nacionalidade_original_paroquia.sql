{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="estrangeiro_nacionalidade_original_paroquia",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1872, "end": 1877, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_provincia as string) id_provincia,
    safe_cast(id_municipio_1872 as string) id_municipio_1872,
    safe_cast(id_paroquia as string) id_paroquia,
    safe_cast(id_categoria as string) id_categoria,
    safe_cast(homens_catolicos_solteiros as int64) homens_catolicos_solteiros,
    safe_cast(homens_catolicos_casados as int64) homens_catolicos_casados,
    safe_cast(homens_catolicos_viuvos as int64) homens_catolicos_viuvos,
    safe_cast(homens_acatolicos_solteiros as int64) homens_acatolicos_solteiros,
    safe_cast(homens_acatolicos_casados as int64) homens_acatolicos_casados,
    safe_cast(homens_acatolicos_viuvos as int64) homens_acatolicos_viuvos,
    safe_cast(mulheres_catolicas_solteiras as int64) mulheres_catolicas_solteiras,
    safe_cast(mulheres_catolicas_casadas as int64) mulheres_catolicas_casadas,
    safe_cast(mulheres_catolicas_viuvas as int64) mulheres_catolicas_viuvas,
    safe_cast(mulheres_acatolicas_solteiras as int64) mulheres_acatolicas_solteiras,
    safe_cast(mulheres_acatolicas_casadas as int64) mulheres_acatolicas_casadas,
    safe_cast(mulheres_acatolicas_viuvas as int64) mulheres_acatolicas_viuvas,
    safe_cast(total as int64) total
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.estrangeiro_nacionalidade_original_paroquia"
        )
    }}
    as t
