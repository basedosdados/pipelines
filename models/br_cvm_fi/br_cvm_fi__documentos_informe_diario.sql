{{
    config(
        schema="br_cvm_fi",
        alias="documentos_informe_diario",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "id_fundo"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_fundo as string) id_fundo,
    regexp_replace(cnpj, r'[^0-9]', '') as cnpj,
    substr(regexp_replace(cnpj, r'[^0-9]', ''), 1, 8) as cnpj_basico,
    safe_cast(data_competencia as date) data_competencia,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(valor_cota as float64) valor_cota,
    safe_cast(valor_patrimonio_liquido as float64) valor_patrimonio_liquido,
    safe_cast(captacao_dia as float64) captacao_dia,
    safe_cast(regate_dia as float64) resgate_dia,
    safe_cast(quantidade_cotistas as int64) quantidade_cotistas,
from {{ set_datalake_project("br_cvm_fi_staging.documentos_informe_diario") }} as t
