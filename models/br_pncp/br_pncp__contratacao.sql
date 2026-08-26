{{
    config(
        schema="br_pncp",
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
    safe_cast(id_contratacao_pncp as string) id_contratacao_pncp,
    safe_cast(ano_compra as int64) ano_compra,
    safe_cast(sequencial_compra as string) sequencial_compra,
    safe_cast(numero_compra as string) numero_compra,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(id_esfera as string) id_esfera,
    safe_cast(id_poder as string) id_poder,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(nome_unidade as string) nome_unidade,
    safe_cast(cnpj_orgao_subrogado as string) cnpj_orgao_subrogado,
    safe_cast(nome_orgao_subrogado as string) nome_orgao_subrogado,
    safe_cast(codigo_unidade_subrogada as string) codigo_unidade_subrogada,
    safe_cast(nome_unidade_subrogada as string) nome_unidade_subrogada,
    safe_cast(id_modalidade as string) id_modalidade,
    safe_cast(modalidade as string) modalidade,
    safe_cast(id_modo_disputa as string) id_modo_disputa,
    safe_cast(modo_disputa as string) modo_disputa,
    safe_cast(id_situacao_compra as string) id_situacao_compra,
    safe_cast(situacao_compra as string) situacao_compra,
    safe_cast(
        id_tipo_instrumento_convocatorio as string
    ) id_tipo_instrumento_convocatorio,
    safe_cast(tipo_instrumento_convocatorio as string) tipo_instrumento_convocatorio,
    safe_cast(codigo_amparo_legal as string) codigo_amparo_legal,
    safe_cast(nome_amparo_legal as string) nome_amparo_legal,
    safe_cast(objeto_compra as string) objeto_compra,
    safe_cast(informacao_complementar as string) informacao_complementar,
    safe_cast(justificativa_presencial as string) justificativa_presencial,
    safe_cast(indicador_srp as boolean) indicador_srp,
    safe_cast(indicador_emenda_parlamentar as boolean) indicador_emenda_parlamentar,
    safe_cast(data_abertura_proposta as date) data_abertura_proposta,
    safe_cast(data_encerramento_proposta as date) data_encerramento_proposta,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_atualizacao as date) data_atualizacao,
    safe_cast(valor_total_estimado as float64) valor_total_estimado,
    safe_cast(valor_total_homologado as float64) valor_total_homologado,
    safe_cast(link_sistema_origem as string) link_sistema_origem
from {{ set_datalake_project("br_pncp_staging.contratacao") }} as t
