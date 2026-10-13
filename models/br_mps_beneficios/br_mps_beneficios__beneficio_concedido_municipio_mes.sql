{{
    config(
        alias="beneficio_concedido_municipio_mes",
        schema="br_mps_beneficios",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2012, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf", "especie_beneficio"],
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(especie_beneficio as string) especie_beneficio,
    safe_cast(categoria_beneficio as string) categoria_beneficio,
    safe_cast(clientela as string) clientela,
    safe_cast(sexo as string) sexo,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(quantidade as int64) quantidade,
    safe_cast(valor_total_salarios_minimos as float64) valor_total_salarios_minimos,
    safe_cast(valor_total as float64) valor_total
from
    {{
        set_datalake_project(
            "br_mps_beneficios_staging.beneficio_concedido_municipio_mes"
        )
    }} as t
