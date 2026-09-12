{{
    config(
        alias="municipio_tipo_especie_eixos",
        schema="br_senatran_estatisticas",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 2013,
                "end": 2031,
                "interval": 1,
            },
        },
        cluster_by=["mes"],
        pre_hook="{% if adapter.get_relation(this.database, this.schema, this.identifier) %}DROP ALL ROW ACCESS POLICIES ON {{ this }}{% else %}SELECT 1{% endif %}",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(lower(tipo_veiculo) as string) tipo_veiculo,
    safe_cast(lower(especie) as string) especie,
    safe_cast(lower(eixos) as string) eixos,
    safe_cast(quantidade as int64) quantidade
from
    {{
        set_datalake_project(
            "br_senatran_estatisticas_staging.municipio_tipo_especie_eixos"
        )
    }} as t
