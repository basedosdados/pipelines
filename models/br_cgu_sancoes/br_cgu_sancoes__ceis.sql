{{
    config(
        schema="br_cgu_sancoes",
        alias="ceis",
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
    safe_cast(cadastro as string) cadastro,
    safe_cast(codigo_sancao as string) codigo_sancao,
    safe_cast(tipo_pessoa as string) tipo_pessoa,
    safe_cast(cpf_cnpj_sancionado as string) cpf_cnpj_sancionado,
    safe_cast(nome_sancionado as string) nome_sancionado,
    safe_cast(nome_informado_orgao as string) nome_informado_orgao,
    safe_cast(razao_social_receita as string) razao_social_receita,
    safe_cast(nome_fantasia_receita as string) nome_fantasia_receita,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(categoria_sancao as string) categoria_sancao,
    safe_cast(data_inicio_sancao as date) data_inicio_sancao,
    safe_cast(data_final_sancao as date) data_final_sancao,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(publicacao as string) publicacao,
    safe_cast(detalhamento_meio_publicacao as string) detalhamento_meio_publicacao,
    safe_cast(data_transito_julgado as date) data_transito_julgado,
    safe_cast(abrangencia_sancao as string) abrangencia_sancao,
    safe_cast(orgao_sancionador as string) orgao_sancionador,
    safe_cast(sigla_uf_orgao as string) sigla_uf_orgao,
    safe_cast(esfera_orgao as string) esfera_orgao,
    safe_cast(fundamentacao_legal as string) fundamentacao_legal,
    safe_cast(data_origem_informacao as date) data_origem_informacao,
    safe_cast(origem_informacao as string) origem_informacao,
    safe_cast(observacoes as string) observacoes
from {{ set_datalake_project("br_cgu_sancoes_staging.ceis") }} as t
