{{
    config(
        schema="br_mapbiomas_estatisticas",
        alias="transicao_uf_de_para_quinquenal",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(ano_inicial as int64) ano_inicial,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_classe_de as string) id_classe_de,
    safe_cast(id_classe_para as string) id_classe_para,
    safe_cast(bioma as string) bioma,
    safe_cast(area as float64) area
from
    {{
        set_datalake_project(
            "br_mapbiomas_estatisticas_staging.transicao_uf_de_para_quinquenal"
        )
    }} as t
