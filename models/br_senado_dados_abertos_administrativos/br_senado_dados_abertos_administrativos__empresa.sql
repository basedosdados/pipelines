{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="empresa",
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
    safe_cast(id_empresa as string) id_empresa,
    safe_cast(cpf_cnpj as string) cpf_cnpj,
    safe_cast(nome as string) nome
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.empresa"
        )
    }} as t
