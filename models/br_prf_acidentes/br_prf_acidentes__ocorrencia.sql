{{
    config(
        schema="br_prf_acidentes",
        alias="ocorrencia",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(data as date) data,
    safe_cast(horario as time) horario,
    safe_cast(dia_semana as string) dia_semana,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_ocorrencia as string) id_ocorrencia,
    safe_cast(br as string) br,
    safe_cast(km as float64) km,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(causa_acidente as string) causa_acidente,
    safe_cast(tipo_acidente as string) tipo_acidente,
    safe_cast(classificacao_acidente as string) classificacao_acidente,
    safe_cast(fase_dia as string) fase_dia,
    safe_cast(sentido_via as string) sentido_via,
    safe_cast(condicao_metereologica as string) condicao_metereologica,
    safe_cast(tipo_pista as string) tipo_pista,
    safe_cast(tracado_via as string) tracado_via,
    safe_cast(uso_solo as string) uso_solo,
    safe_cast(quantidade_pessoas as int64) quantidade_pessoas,
    safe_cast(quantidade_mortos as int64) quantidade_mortos,
    safe_cast(quantidade_feridos_leves as int64) quantidade_feridos_leves,
    safe_cast(quantidade_feridos_graves as int64) quantidade_feridos_graves,
    safe_cast(quantidade_ilesos as int64) quantidade_ilesos,
    safe_cast(quantidade_ignorados as int64) quantidade_ignorados,
    safe_cast(quantidade_feridos as int64) quantidade_feridos,
    safe_cast(quantidade_veiculos as int64) quantidade_veiculos,
    safe_cast(regional as string) regional,
    safe_cast(delegacia as string) delegacia,
    safe_cast(uop as string) uop
from {{ set_datalake_project("br_prf_acidentes_staging.ocorrencia") }} as t
