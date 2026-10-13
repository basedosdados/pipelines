{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="mulher_origem_brasileira_corrigido_paroquia",
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
    safe_cast(solteiras_brancas_livres as int64) solteiras_brancas_livres,
    safe_cast(solteiras_pardas_livres as int64) solteiras_pardas_livres,
    safe_cast(solteiras_pretas_livres as int64) solteiras_pretas_livres,
    safe_cast(solteiras_caboclas_livres as int64) solteiras_caboclas_livres,
    safe_cast(casadas_brancas_livres as int64) casadas_brancas_livres,
    safe_cast(casadas_pardas_livres as int64) casadas_pardas_livres,
    safe_cast(casadas_pretas_livres as int64) casadas_pretas_livres,
    safe_cast(casadas_caboclas_livres as int64) casadas_caboclas_livres,
    safe_cast(viuvas_brancas_livres as int64) viuvas_brancas_livres,
    safe_cast(viuvas_pardas_livres as int64) viuvas_pardas_livres,
    safe_cast(viuvas_pretas_livres as int64) viuvas_pretas_livres,
    safe_cast(viuvas_caboclas_livres as int64) viuvas_caboclas_livres,
    safe_cast(livres_sem_informacao as int64) livres_sem_informacao,
    safe_cast(solteiras_pardas_escravizadas as int64) solteiras_pardas_escravizadas,
    safe_cast(solteiras_pretas_escravizadas as int64) solteiras_pretas_escravizadas,
    safe_cast(casadas_pardas_escravizadas as int64) casadas_pardas_escravizadas,
    safe_cast(casadas_pretas_escravizadas as int64) casadas_pretas_escravizadas,
    safe_cast(viuvas_pardas_escravizadas as int64) viuvas_pardas_escravizadas,
    safe_cast(viuvas_pretas_escravizadas as int64) viuvas_pretas_escravizadas,
    safe_cast(escravizadas_sem_informacao as int64) escravizadas_sem_informacao
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.mulher_origem_brasileira_corrigido_paroquia"
        )
    }}
    as t
