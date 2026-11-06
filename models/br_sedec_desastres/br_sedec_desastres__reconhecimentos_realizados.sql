{{
    config(
        schema="br_sedec_desastres",
        alias="reconhecimentos_realizados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) as ano,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(id_municipio as string) as id_municipio,
    safe_cast(id_cobrade as string) as id_cobrade,
    safe_cast(nome_cobrade as string) as nome_cobrade,
    safe_cast(situacao as string) as situacao,
    safe_cast(rito as string) as rito,
    safe_cast(numero_decreto as string) as numero_decreto,
    safe_cast(data_decreto as date) as data_decreto,
    safe_cast(numero_portaria as string) as numero_portaria,
    safe_cast(data_portaria as date) as data_portaria,
    safe_cast(numero_dou as string) as numero_dou,
    safe_cast(data_dou as date) as data_dou,
    safe_cast(numero_processo as string) as numero_processo
from
    {{ set_datalake_project("br_sedec_desastres_staging.reconhecimentos_realizados") }}
    as t
