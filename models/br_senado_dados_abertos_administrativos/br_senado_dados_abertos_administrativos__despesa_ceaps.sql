{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="despesa_ceaps",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_despesa as string) id_despesa,
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome_senador as string) nome_senador,
    safe_cast(tipo_despesa as string) tipo_despesa,
    safe_cast(tipo_documento as string) tipo_documento,
    safe_cast(documento as string) documento,
    safe_cast(data as date) data,
    safe_cast(cpf_cnpj_fornecedor as string) cpf_cnpj_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(detalhamento as string) detalhamento,
    safe_cast(valor_reembolsado as float64) valor_reembolsado
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.despesa_ceaps"
        )
    }} as t
