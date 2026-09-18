{{
    config(
        schema="br_mgi_compras_publicas",
        alias="pregao_item_oferta",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_item as string) numero_item,
    safe_cast(cnpj_cpf_fornecedor as string) cnpj_cpf_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(unidade_fornecimento as string) unidade_fornecimento,
    safe_cast(quantidade as float64) quantidade,
    safe_cast(valor_unitario as float64) valor_unitario,
    safe_cast(valor_global as float64) valor_global,
    safe_cast(marca as string) marca,
    safe_cast(fabricante as string) fabricante,
    safe_cast(modelo_versao as string) modelo_versao,
    safe_cast(descricao_detalhada_ofertada as string) descricao_detalhada_ofertada
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.pregao_item_oferta") }}
    as t
qualify
    row_number() over (
        partition by ano, id_compra, numero_item, cnpj_cpf_fornecedor
        order by cast(valor_global as string) desc
    )
    = 1
