{{
    config(
        schema="br_tse_eleicoes",
        alias="pesquisa_eleitoral",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2012, "end": 2030, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(id_pesquisa as string) id_pesquisa,
    safe_cast(cnpj_empresa as string) cnpj_empresa,
    safe_cast(nome_empresa as string) nome_empresa,
    safe_cast(nome_fantasia_empresa as string) nome_fantasia_empresa,
    safe_cast(pesquisa_propria as string) pesquisa_propria,
    safe_cast(cargos as string) cargos,
    safe_cast(data_registro as date) data_registro,
    safe_cast(hora_registro as time) hora_registro,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(data_divulgacao as date) data_divulgacao,
    safe_cast(quantidade_entrevistados as int64) quantidade_entrevistados,
    safe_cast(valor_pesquisa as float64) valor_pesquisa,
    safe_cast(registro_conre_estatistico as string) registro_conre_estatistico,
    safe_cast(nome_estatistico as string) nome_estatistico,
    safe_cast(descricao_metodologia as string) descricao_metodologia,
    safe_cast(descricao_plano_amostral as string) descricao_plano_amostral,
    safe_cast(descricao_sistema_controle as string) descricao_sistema_controle,
    safe_cast(descricao_area_abrangencia as string) descricao_area_abrangencia
from {{ set_datalake_project("br_tse_eleicoes_staging.pesquisa_eleitoral") }} as t
