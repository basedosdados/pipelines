{{
    config(
        alias="lavoura_permanente",
        schema="br_ibge_pam",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1974, "end": 2022, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(produto as string) produto,
    safe_cast(area_destinada_colheita as int64) area_destinada_colheita,
    safe_cast(area_colhida as int64) area_colhida,
    round(safe_cast(quantidade_produzida as float64), 4) quantidade_produzida,
    round(safe_cast(rendimento_medio_producao as float64), 4) rendimento_medio_producao,
    round(safe_cast(valor_producao as float64), 4) valor_producao
from {{ set_datalake_project("br_ibge_pam_staging.lavoura_permanente") }} as t
