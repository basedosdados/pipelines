{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="profissao_original_paroquia",
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
    safe_cast(homens_brasileiros_solteiros as int64) homens_brasileiros_solteiros,
    safe_cast(homens_brasileiros_casados as int64) homens_brasileiros_casados,
    safe_cast(homens_brasileiros_viuvos as int64) homens_brasileiros_viuvos,
    safe_cast(mulheres_brasileiras_solteiras as int64) mulheres_brasileiras_solteiras,
    safe_cast(mulheres_brasileiras_casadas as int64) mulheres_brasileiras_casadas,
    safe_cast(mulheres_brasileiras_viuvas as int64) mulheres_brasileiras_viuvas,
    safe_cast(homens_estrangeiros_solteiros as int64) homens_estrangeiros_solteiros,
    safe_cast(homens_estrangeiros_casados as int64) homens_estrangeiros_casados,
    safe_cast(homens_estrangeiros_viuvos as int64) homens_estrangeiros_viuvos,
    safe_cast(mulheres_estrangeiras_solteiras as int64) mulheres_estrangeiras_solteiras,
    safe_cast(mulheres_estrangeiras_casadas as int64) mulheres_estrangeiras_casadas,
    safe_cast(mulheres_estrangeiras_viuvas as int64) mulheres_estrangeiras_viuvas,
    safe_cast(homens_escravizados as int64) homens_escravizados,
    safe_cast(mulheres_escravizadas as int64) mulheres_escravizadas,
    safe_cast(total as int64) total
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.profissao_original_paroquia"
        )
    }} as t
