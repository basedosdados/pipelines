{{
    config(
        schema="br_mgi_compras_publicas",
        alias="catalogo_material",
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
    safe_cast(codigo_item as string) codigo_item,
    safe_cast(codigo_grupo as string) codigo_grupo,
    safe_cast(nome_grupo as string) nome_grupo,
    safe_cast(codigo_classe as string) codigo_classe,
    safe_cast(nome_classe as string) nome_classe,
    safe_cast(codigo_pdm as string) codigo_pdm,
    safe_cast(nome_pdm as string) nome_pdm,
    safe_cast(codigo_ncm as string) codigo_ncm,
    safe_cast(descricao_ncm as string) descricao_ncm,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(indicador_item_sustentavel as boolean) indicador_item_sustentavel,
    safe_cast(
        indicador_aplica_margem_preferencia as boolean
    ) indicador_aplica_margem_preferencia,
    safe_cast(indicador_item_ativo as boolean) indicador_item_ativo,
    safe_cast(data_hora_atualizacao as datetime) data_hora_atualizacao
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.catalogo_material") }} as t
qualify
    row_number() over (
        partition by data_extracao, codigo_item order by data_hora_atualizacao desc
    )
    = 1
