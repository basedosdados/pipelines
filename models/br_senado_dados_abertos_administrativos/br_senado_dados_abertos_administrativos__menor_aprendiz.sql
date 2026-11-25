{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="menor_aprendiz",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(cpf as string) cpf,
    safe_cast(nome as string) nome,
    safe_cast(situacao as string) situacao,
    safe_cast(fornecedor as string) fornecedor,
    safe_cast(sigla_orgao as string) sigla_orgao,
    safe_cast(orgao as string) orgao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.menor_aprendiz"
        )
    }} as t
