{{
    config(
        schema="br_mj_sinesp",
        alias="uf_mes",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(tipo_ocorrencia as string) tipo_ocorrencia,
    safe_cast(abrangencia as string) abrangencia,
    safe_cast(arma as string) arma,
    safe_cast(agente as string) agente,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(quantidade_ocorrencias as int64) quantidade_ocorrencias,
    safe_cast(quantidade_vitimas as int64) quantidade_vitimas,
    safe_cast(quantidade_vitimas_feminino as int64) quantidade_vitimas_feminino,
    safe_cast(quantidade_vitimas_masculino as int64) quantidade_vitimas_masculino,
    safe_cast(
        quantidade_vitimas_sexo_nao_informado as int64
    ) quantidade_vitimas_sexo_nao_informado,
    safe_cast(peso_apreendido as float64) peso_apreendido
from {{ set_datalake_project("br_mj_sinesp_staging.uf_mes") }} as t
