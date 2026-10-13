{{
    config(
        schema="br_mgi_compras_publicas",
        alias="orgao",
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
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(nome_mnemonico_orgao as string) nome_mnemonico_orgao,
    safe_cast(codigo_orgao_vinculado as string) codigo_orgao_vinculado,
    safe_cast(cnpj_orgao_vinculado as string) cnpj_orgao_vinculado,
    safe_cast(nome_orgao_vinculado as string) nome_orgao_vinculado,
    safe_cast(codigo_orgao_superior as string) codigo_orgao_superior,
    safe_cast(cnpj_orgao_superior as string) cnpj_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_tipo_administracao as string) codigo_tipo_administracao,
    safe_cast(nome_tipo_administracao as string) nome_tipo_administracao,
    safe_cast(poder as string) poder,
    safe_cast(esfera as string) esfera,
    safe_cast(indicador_uso_sisg as boolean) indicador_uso_sisg,
    safe_cast(indicador_orgao_ativo as boolean) indicador_orgao_ativo,
    safe_cast(data_hora_movimento as datetime) data_hora_movimento
from {{ set_datalake_project("br_mgi_compras_publicas_staging.orgao") }} as t
qualify
    row_number() over (
        partition by
            data_extracao,
            codigo_orgao,
            cnpj_orgao,
            nome_orgao,
            nome_mnemonico_orgao,
            codigo_orgao_vinculado,
            cnpj_orgao_vinculado,
            nome_orgao_vinculado,
            codigo_orgao_superior,
            cnpj_orgao_superior,
            nome_orgao_superior,
            codigo_tipo_administracao,
            nome_tipo_administracao,
            poder,
            esfera,
            indicador_uso_sisg,
            indicador_orgao_ativo,
            data_hora_movimento
        order by data_hora_movimento desc
    )
    = 1
