{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="populacao_geral_corrigido_provincia",
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
    safe_cast(id_categoria as string) id_categoria,
    safe_cast(homens_livres as int64) homens_livres,
    safe_cast(mulheres_livres as int64) mulheres_livres,
    safe_cast(total_livres as int64) total_livres,
    safe_cast(homens_escravizados as int64) homens_escravizados,
    safe_cast(mulheres_escravizadas as int64) mulheres_escravizadas,
    safe_cast(total_escravizados as int64) total_escravizados,
    safe_cast(total as int64) total
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.populacao_geral_corrigido_provincia"
        )
    }} as t
