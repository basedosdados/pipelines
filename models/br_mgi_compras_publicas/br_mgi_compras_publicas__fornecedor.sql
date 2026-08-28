{{
    config(
        schema="br_mgi_compras_publicas",
        alias="fornecedor",
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
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(cnpj as string) cnpj,
    safe_cast(cpf as string) cpf,
    safe_cast(nome_razao_social as string) nome_razao_social,
    safe_cast(codigo_cnae as string) codigo_cnae,
    safe_cast(nome_cnae as string) nome_cnae,
    safe_cast(id_natureza_juridica as string) id_natureza_juridica,
    safe_cast(natureza_juridica as string) natureza_juridica,
    safe_cast(id_porte_empresa as string) id_porte_empresa,
    safe_cast(porte_empresa as string) porte_empresa,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(indicador_habilitado_licitar as boolean) indicador_habilitado_licitar,
    safe_cast(indicador_fornecedor_ativo as boolean) indicador_fornecedor_ativo
from {{ set_datalake_project("br_mgi_compras_publicas_staging.fornecedor") }} as t
qualify
    row_number() over (
        partition by data_extracao, cnpj, cpf, nome_razao_social order by cnpj desc
    )
    = 1
