{{
    config(
        schema="br_mgi_compras_publicas",
        alias="contratacao_item",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_compra as string) id_compra,
    safe_cast(id_compra_item as string) id_compra_item,
    safe_cast(numero_controle_pncp as string) numero_controle_pncp,
    safe_cast(id_contratacao_pncp as string) id_contratacao_pncp,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(numero_item_pncp as string) numero_item_pncp,
    safe_cast(numero_item_compra as string) numero_item_compra,
    safe_cast(numero_grupo as string) numero_grupo,
    safe_cast(tipo_item as string) tipo_item,
    safe_cast(nome_tipo_item as string) nome_tipo_item,
    safe_cast(codigo_item_catalogo as string) codigo_item_catalogo,
    safe_cast(codigo_grupo as string) codigo_grupo,
    safe_cast(codigo_classe as string) codigo_classe,
    safe_cast(codigo_pdm as string) codigo_pdm,
    safe_cast(nome_pdm as string) nome_pdm,
    safe_cast(codigo_ncm as string) codigo_ncm,
    safe_cast(descricao_ncm as string) descricao_ncm,
    safe_cast(descricao_resumida as string) descricao_resumida,
    safe_cast(descricao_detalhada as string) descricao_detalhada,
    safe_cast(unidade_medida as string) unidade_medida,
    safe_cast(id_item_categoria as string) id_item_categoria,
    safe_cast(item_categoria as string) item_categoria,
    safe_cast(id_criterio_julgamento as string) id_criterio_julgamento,
    safe_cast(criterio_julgamento as string) criterio_julgamento,
    safe_cast(id_situacao_item as string) id_situacao_item,
    safe_cast(situacao_item as string) situacao_item,
    safe_cast(id_tipo_beneficio as string) id_tipo_beneficio,
    safe_cast(tipo_beneficio as string) tipo_beneficio,
    safe_cast(quantidade as float64) quantidade,
    safe_cast(valor_unitario_estimado as float64) valor_unitario_estimado,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(quantidade_resultado as float64) quantidade_resultado,
    safe_cast(valor_unitario_resultado as float64) valor_unitario_resultado,
    safe_cast(valor_total_resultado as float64) valor_total_resultado,
    safe_cast(id_fornecedor as string) id_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(
        percentual_margem_preferencia_normal as float64
    ) percentual_margem_preferencia_normal,
    safe_cast(
        percentual_margem_preferencia_adicional as float64
    ) percentual_margem_preferencia_adicional,
    safe_cast(indicador_orcamento_sigiloso as boolean) indicador_orcamento_sigiloso,
    safe_cast(
        indicador_incentivo_produtivo_basico as boolean
    ) indicador_incentivo_produtivo_basico,
    safe_cast(
        indicador_margem_preferencia_normal as boolean
    ) indicador_margem_preferencia_normal,
    safe_cast(
        indicador_margem_preferencia_adicional as boolean
    ) indicador_margem_preferencia_adicional,
    safe_cast(indicador_tem_resultado as boolean) indicador_tem_resultado,
    safe_cast(data_inclusao_pncp as datetime) data_inclusao_pncp,
    safe_cast(data_atualizacao_pncp as datetime) data_atualizacao_pncp,
    safe_cast(data_resultado as datetime) data_resultado
from {{ set_datalake_project("br_mgi_compras_publicas_staging.contratacao_item") }} as t
qualify
    row_number() over (
        partition by
            ano,
            id_compra,
            id_compra_item,
            numero_controle_pncp,
            id_contratacao_pncp,
            cnpj_orgao,
            codigo_unidade,
            numero_item_pncp,
            numero_item_compra,
            numero_grupo,
            tipo_item,
            nome_tipo_item,
            codigo_item_catalogo,
            codigo_grupo,
            codigo_classe,
            codigo_pdm,
            nome_pdm,
            codigo_ncm,
            descricao_ncm,
            descricao_resumida,
            descricao_detalhada,
            unidade_medida,
            id_item_categoria,
            item_categoria,
            id_criterio_julgamento,
            criterio_julgamento,
            id_situacao_item,
            situacao_item,
            id_tipo_beneficio,
            tipo_beneficio,
            cast(quantidade as string),
            cast(valor_unitario_estimado as string),
            cast(valor_total as string),
            cast(quantidade_resultado as string),
            cast(valor_unitario_resultado as string),
            cast(valor_total_resultado as string),
            id_fornecedor,
            nome_fornecedor,
            cast(percentual_margem_preferencia_normal as string),
            cast(percentual_margem_preferencia_adicional as string),
            indicador_orcamento_sigiloso,
            indicador_incentivo_produtivo_basico,
            indicador_margem_preferencia_normal,
            indicador_margem_preferencia_adicional,
            indicador_tem_resultado,
            data_inclusao_pncp,
            data_atualizacao_pncp,
            data_resultado
        order by data_atualizacao_pncp desc
    )
    = 1
