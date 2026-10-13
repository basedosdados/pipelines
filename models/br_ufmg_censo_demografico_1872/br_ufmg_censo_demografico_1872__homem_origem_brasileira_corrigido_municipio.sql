{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="homem_origem_brasileira_corrigido_municipio",
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
    safe_cast(id_categoria as string) id_categoria,
    safe_cast(solteiros_brancos_livres as int64) solteiros_brancos_livres,
    safe_cast(solteiros_pardos_livres as int64) solteiros_pardos_livres,
    safe_cast(solteiros_pretos_livres as int64) solteiros_pretos_livres,
    safe_cast(solteiros_caboclos_livres as int64) solteiros_caboclos_livres,
    safe_cast(casados_brancos_livres as int64) casados_brancos_livres,
    safe_cast(casados_pardos_livres as int64) casados_pardos_livres,
    safe_cast(casados_pretos_livres as int64) casados_pretos_livres,
    safe_cast(casados_caboclos_livres as int64) casados_caboclos_livres,
    safe_cast(viuvos_brancos_livres as int64) viuvos_brancos_livres,
    safe_cast(viuvos_pardos_livres as int64) viuvos_pardos_livres,
    safe_cast(viuvos_pretos_livres as int64) viuvos_pretos_livres,
    safe_cast(viuvos_caboclos_livres as int64) viuvos_caboclos_livres,
    safe_cast(livres_sem_informacao as int64) livres_sem_informacao,
    safe_cast(solteiros_pardos_escravizados as int64) solteiros_pardos_escravizados,
    safe_cast(solteiros_pretos_escravizados as int64) solteiros_pretos_escravizados,
    safe_cast(casados_pardos_escravizados as int64) casados_pardos_escravizados,
    safe_cast(casados_pretos_escravizados as int64) casados_pretos_escravizados,
    safe_cast(viuvos_pardos_escravizados as int64) viuvos_pardos_escravizados,
    safe_cast(viuvos_pretos_escravizados as int64) viuvos_pretos_escravizados,
    safe_cast(escravizados_sem_informacao as int64) escravizados_sem_informacao
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.homem_origem_brasileira_corrigido_municipio"
        )
    }}
    as t
