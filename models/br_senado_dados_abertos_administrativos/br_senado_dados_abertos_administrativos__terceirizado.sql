{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="terceirizado",
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
    safe_cast(id_contrato as string) id_contrato,
    safe_cast(numero_contrato as string) numero_contrato,
    safe_cast(id_item_contrato as string) id_item_contrato,
    safe_cast(situacao as string) situacao,
    safe_cast(empresa as string) empresa,
    safe_cast(sigla_lotacao as string) sigla_lotacao,
    safe_cast(lotacao as string) lotacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.terceirizado"
        )
    }} as t
