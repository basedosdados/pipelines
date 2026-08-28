{{
    config(
        schema="br_mgi_compras_publicas",
        alias="contratacao",
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
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_controle_pncp as string) numero_controle_pncp,
    safe_cast(ano_compra_pncp as int64) ano_compra_pncp,
    safe_cast(sequencial_compra_pncp as string) sequencial_compra_pncp,
    safe_cast(numero_compra as string) numero_compra,
    safe_cast(processo as string) processo,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(esfera as string) esfera,
    safe_cast(poder as string) poder,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(nome_unidade as string) nome_unidade,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(cnpj_orgao_subrogado as string) cnpj_orgao_subrogado,
    safe_cast(nome_orgao_subrogado as string) nome_orgao_subrogado,
    safe_cast(esfera_subrogado as string) esfera_subrogado,
    safe_cast(poder_subrogado as string) poder_subrogado,
    safe_cast(codigo_unidade_subrogada as string) codigo_unidade_subrogada,
    safe_cast(nome_unidade_subrogada as string) nome_unidade_subrogada,
    safe_cast(sigla_uf_subrogada as string) sigla_uf_subrogada,
    safe_cast(id_municipio_subrogada as string) id_municipio_subrogada,
    safe_cast(nome_municipio_subrogada as string) nome_municipio_subrogada,
    safe_cast(codigo_modalidade as string) codigo_modalidade,
    safe_cast(id_modalidade_pncp as string) id_modalidade_pncp,
    safe_cast(modalidade as string) modalidade,
    safe_cast(id_modo_disputa_pncp as string) id_modo_disputa_pncp,
    safe_cast(codigo_modo_disputa as string) codigo_modo_disputa,
    safe_cast(modo_disputa as string) modo_disputa,
    safe_cast(codigo_amparo_legal as string) codigo_amparo_legal,
    safe_cast(amparo_legal as string) amparo_legal,
    safe_cast(descricao_amparo_legal as string) descricao_amparo_legal,
    safe_cast(
        codigo_tipo_instrumento_convocatorio as string
    ) codigo_tipo_instrumento_convocatorio,
    safe_cast(tipo_instrumento_convocatorio as string) tipo_instrumento_convocatorio,
    safe_cast(id_situacao_compra as string) id_situacao_compra,
    safe_cast(situacao_compra as string) situacao_compra,
    safe_cast(codigo_orcamento_sigiloso as string) codigo_orcamento_sigiloso,
    safe_cast(orcamento_sigiloso as string) orcamento_sigiloso,
    safe_cast(objeto_compra as string) objeto_compra,
    safe_cast(informacao_complementar as string) informacao_complementar,
    safe_cast(valor_total_estimado as float64) valor_total_estimado,
    safe_cast(valor_total_homologado as float64) valor_total_homologado,
    safe_cast(indicador_srp as boolean) indicador_srp,
    safe_cast(indicador_existe_resultado as boolean) indicador_existe_resultado,
    safe_cast(indicador_contratacao_excluida as boolean) indicador_contratacao_excluida,
    safe_cast(data_publicacao_pncp as datetime) data_publicacao_pncp,
    safe_cast(data_inclusao_pncp as datetime) data_inclusao_pncp,
    safe_cast(data_atualizacao_pncp as datetime) data_atualizacao_pncp,
    safe_cast(data_abertura_proposta as datetime) data_abertura_proposta,
    safe_cast(data_encerramento_proposta as datetime) data_encerramento_proposta
from {{ set_datalake_project("br_mgi_compras_publicas_staging.contratacao") }} as t
qualify
    row_number() over (
        partition by numero_controle_pncp order by data_atualizacao_pncp desc
    )
    = 1
