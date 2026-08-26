{{
    config(
        schema="br_pncp",
        alias="plano_contratacao_anual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2023, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_pca_pncp as string) id_pca_pncp,
    safe_cast(numero_item as string) numero_item,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(nome_unidade as string) nome_unidade,
    safe_cast(unidade_requisitante as string) unidade_requisitante,
    safe_cast(codigo_item as string) codigo_item,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(id_classificacao_catalogo as string) id_classificacao_catalogo,
    safe_cast(nome_classificacao_catalogo as string) nome_classificacao_catalogo,
    safe_cast(codigo_classificacao_superior as string) codigo_classificacao_superior,
    safe_cast(nome_classificacao_superior as string) nome_classificacao_superior,
    safe_cast(codigo_pdm as string) codigo_pdm,
    safe_cast(descricao_pdm as string) descricao_pdm,
    safe_cast(codigo_grupo_contratacao as string) codigo_grupo_contratacao,
    safe_cast(nome_grupo_contratacao as string) nome_grupo_contratacao,
    safe_cast(categoria_item as string) categoria_item,
    safe_cast(unidade_fornecimento as string) unidade_fornecimento,
    safe_cast(data_desejada as date) data_desejada,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_atualizacao as date) data_atualizacao,
    safe_cast(quantidade_estimada as float64) quantidade_estimada,
    safe_cast(valor_unitario as float64) valor_unitario,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(valor_orcamento_exercicio as float64) valor_orcamento_exercicio
from {{ set_datalake_project("br_pncp_staging.plano_contratacao_anual") }} as t
