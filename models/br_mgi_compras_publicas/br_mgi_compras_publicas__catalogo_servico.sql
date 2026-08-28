{{
    config(
        schema="br_mgi_compras_publicas",
        alias="catalogo_servico",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(codigo_servico as string) codigo_servico,
    safe_cast(codigo_secao as string) codigo_secao,
    safe_cast(nome_secao as string) nome_secao,
    safe_cast(codigo_divisao as string) codigo_divisao,
    safe_cast(nome_divisao as string) nome_divisao,
    safe_cast(codigo_grupo as string) codigo_grupo,
    safe_cast(nome_grupo as string) nome_grupo,
    safe_cast(codigo_classe as string) codigo_classe,
    safe_cast(nome_classe as string) nome_classe,
    safe_cast(codigo_subclasse as string) codigo_subclasse,
    safe_cast(nome_subclasse as string) nome_subclasse,
    safe_cast(nome_servico as string) nome_servico,
    safe_cast(codigo_cpc as string) codigo_cpc,
    safe_cast(
        indicador_exclusivo_central_compras as boolean
    ) indicador_exclusivo_central_compras,
    safe_cast(indicador_servico_ativo as boolean) indicador_servico_ativo,
    safe_cast(data_hora_atualizacao as datetime) data_hora_atualizacao
from {{ set_datalake_project("br_mgi_compras_publicas_staging.catalogo_servico") }} as t
qualify
    row_number() over (
        partition by data_extracao, codigo_servico order by data_hora_atualizacao desc
    )
    = 1
