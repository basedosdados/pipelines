{{
    config(
        schema="br_mj_sinesp",
        alias="municipio_mes",
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
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_ocorrencia as string) tipo_ocorrencia,
    safe_cast(abrangencia as string) abrangencia,
    safe_cast(situacao_registro as string) situacao_registro,
    safe_cast(quantidade_ocorrencias as int64) quantidade_ocorrencias,
    safe_cast(quantidade_vitimas as int64) quantidade_vitimas,
    safe_cast(quantidade_vitimas_feminino as int64) quantidade_vitimas_feminino,
    safe_cast(quantidade_vitimas_masculino as int64) quantidade_vitimas_masculino,
    safe_cast(
        quantidade_vitimas_sexo_nao_informado as int64
    ) quantidade_vitimas_sexo_nao_informado
from {{ set_datalake_project("br_mj_sinesp_staging.municipio_mes") }} as t
