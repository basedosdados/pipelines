{{
    config(
        schema="br_prf_acidentes",
        alias="pessoa_causa_tipo",
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
    safe_cast(id_veiculo as string) id_veiculo,
    safe_cast(id_pessoa as string) id_pessoa,
    safe_cast(br as string) br,
    safe_cast(km as float64) km,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(causa_principal as string) causa_principal,
    safe_cast(causa_acidente as string) causa_acidente,
    safe_cast(ordem_tipo_acidente as string) ordem_tipo_acidente,
    safe_cast(tipo_acidente as string) tipo_acidente,
    safe_cast(classificacao_acidente as string) classificacao_acidente,
    safe_cast(fase_dia as string) fase_dia,
    safe_cast(sentido_via as string) sentido_via,
    safe_cast(condicao_metereologica as string) condicao_metereologica,
    safe_cast(tipo_pista as string) tipo_pista,
    safe_cast(tracado_via as string) tracado_via,
    safe_cast(uso_solo as string) uso_solo,
    safe_cast(tipo_veiculo as string) tipo_veiculo,
    safe_cast(marca as string) marca,
    safe_cast(ano_fabricacao_veiculo as int64) ano_fabricacao_veiculo,
    safe_cast(tipo_envolvido as string) tipo_envolvido,
    safe_cast(estado_fisico as string) estado_fisico,
    safe_cast(idade as int64) idade,
    safe_cast(sexo as string) sexo,
    safe_cast(indicador_ileso as string) indicador_ileso,
    safe_cast(indicador_ferido_leve as string) indicador_ferido_leve,
    safe_cast(indicador_ferido_grave as string) indicador_ferido_grave,
    safe_cast(indicador_morto as string) indicador_morto,
    safe_cast(regional as string) regional,
    safe_cast(delegacia as string) delegacia,
    safe_cast(uop as string) uop
from {{ set_datalake_project("br_prf_acidentes_staging.pessoa_causa_tipo") }} as t
