{{
    config(
        schema="br_cgu_sancoes",
        alias="cepim",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(cnpj_entidade as string) cnpj_entidade,
    safe_cast(nome_entidade as string) nome_entidade,
    safe_cast(numero_convenio as string) numero_convenio,
    safe_cast(orgao_concedente as string) orgao_concedente,
    safe_cast(motivo_impedimento as string) motivo_impedimento
from {{ set_datalake_project("br_cgu_sancoes_staging.cepim") }} as t
