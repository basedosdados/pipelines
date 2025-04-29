{{
    config(
        alias="historico_jurados",
        schema="world_oceanos_mapeamento",
        materialized="table",
    )
}}

select
    safe_cast(ano as integer) ano,
    safe_cast(nome_final as string) nome,
    safe_cast(nome_pais as string) nome_pais,
    safe_cast(replace(sigla_uf, "-", "") as string) sigla_uf,
    safe_cast(nome_municipio_origem as string) nome_municipio_origem,
    safe_cast(genero as string) genero,
    safe_cast(ocupacao_match_1 as string) ocupacao,
    safe_cast(instituicao as string) instituicao,
    safe_cast(replace(categoria, "-", "") as string) categoria,
    safe_cast(indicador_juri_intermediario as string) indicador_juri_intermediario,
    safe_cast(indicador_juri_final as string) indicador_juri_final,
from
    {{ set_datalake_project("world_oceanos_mapeamento_staging.historico_jurados") }}
    as t
