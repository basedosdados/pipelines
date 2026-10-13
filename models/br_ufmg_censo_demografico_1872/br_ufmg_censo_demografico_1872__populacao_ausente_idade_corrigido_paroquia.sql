{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="populacao_ausente_idade_corrigido_paroquia",
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
    safe_cast(homens_brancos_livres as int64) homens_brancos_livres,
    safe_cast(homens_pardos_livres as int64) homens_pardos_livres,
    safe_cast(homens_pretos_livres as int64) homens_pretos_livres,
    safe_cast(homens_caboclos_livres as int64) homens_caboclos_livres,
    safe_cast(homens_pardos_escravizados as int64) homens_pardos_escravizados,
    safe_cast(homens_pretos_escravizados as int64) homens_pretos_escravizados,
    safe_cast(mulheres_brancas_livres as int64) mulheres_brancas_livres,
    safe_cast(mulheres_pardas_livres as int64) mulheres_pardas_livres,
    safe_cast(mulheres_pretas_livres as int64) mulheres_pretas_livres,
    safe_cast(mulheres_caboclas_livres as int64) mulheres_caboclas_livres,
    safe_cast(mulheres_pardas_escravizadas as int64) mulheres_pardas_escravizadas,
    safe_cast(mulheres_pretas_escravizadas as int64) mulheres_pretas_escravizadas,
    safe_cast(total as int64) total
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.populacao_ausente_idade_corrigido_paroquia"
        )
    }}
    as t
