{{
    config(
        alias="votacao_objeto",
        schema="br_camara_dados_abertos",
        materialized="table",
    )
}}

select
    safe_cast(replace(idvotacao, "-", "") as string) id_votacao,
    safe_cast(data as date) data,
    safe_cast(
        replace(descricao, "Name: descricao, dtype: object", "") as string
    ) descricao,
    safe_cast(proposicao_id as string) id_proposicao,
    safe_cast(replace(proposicao_ano, ".0", "") as int64) ano_proposicao,
    safe_cast(proposicao_ementa as string) ementa,
    safe_cast(proposicao_codtipo as string) codigo_tipo,
    safe_cast(proposicao_siglatipo as string) sigla_tipo,
    safe_cast(replace(proposicao_numero, ".0", "") as string) numero,
    safe_cast(proposicao_titulo as string) titulo
from {{ set_datalake_project("br_camara_dados_abertos_staging.votacao_objeto") }} as t
