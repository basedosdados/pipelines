{{
    config(
        alias="proposicao_tema",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1935, "end": 2024, "interval": 1},
        },
    )
}}

with
    tables as (
        select
            safe_cast(replace(ano, ".0", "") as int64) as ano,
            regexp_extract(uriproposicao, r'/proposicoes/(\d+)') as id_proposicao,
            safe_cast(siglatipo as string) as tipo_proposicao,
            safe_cast(numero as string) as numero,
            safe_cast(tema as string) as tema,
            safe_cast(relevancia as int64) as relevancia
        from
            {{
                set_datalake_project(
                    "br_camara_dados_abertos_staging.proposicao_tema"
                )
            }}
    )
select *
from tables
where not (ano = 2011 and id_proposicao = '510035')
