{{
    config(
        schema="br_tse_eleicoes",
        alias="perfil_eleitorado_municipio_zona",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(situacao_biometria as string) situacao_biometria,
    safe_cast(zona as string) zona,
    safe_cast(genero as string) genero,
    safe_cast(estado_civil as string) estado_civil,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(instrucao as string) instrucao,
    safe_cast(eleitores as string) eleitores,
    safe_cast(eleitores_biometria as string) eleitores_biometria,
    safe_cast(eleitores_deficiencia as string) eleitores_deficiencia
from
    {{
        set_datalake_project(
            "br_tse_eleicoes_staging.perfil_eleitorado_municipio_zona"
        )
    }} as t
