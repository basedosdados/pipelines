{{
    config(
        schema="br_tse_eleicoes",
        alias="receitas_comite",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2014, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(tipo_comite as string) tipo_comite,
    safe_cast(sequencial_comite as string) sequencial_comite,
    safe_cast(numero_partido as string) numero_partido,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(data_receita as date) data_receita,
    safe_cast(origem_receita as string) origem_receita,
    safe_cast(fonte_receita as string) fonte_receita,
    safe_cast(natureza_receita as string) natureza_receita,
    safe_cast(situacao_receita as string) situacao_receita,
    safe_cast(descricao_receita as string) descricao_receita,
    safe_cast(tipo_documento as string) tipo_documento,
    safe_cast(numero_documento as string) numero_documento,
    safe_cast(nome_membro as string) nome_membro,
    safe_cast(cpf_membro as string) cpf_membro,
    safe_cast(cnpj_prestador_contas as string) cnpj_prestador_contas,
    safe_cast(sigla_uf_doador as string) sigla_uf_doador,
    safe_cast(nome_doador as string) nome_doador,
    safe_cast(nome_doador_rf as string) nome_doador_rf,
    safe_cast(cpf_cnpj_doador as string) cpf_cnpj_doador,
    safe_cast(numero_partido_doador as string) numero_partido_doador,
    safe_cast(numero_candidato_doador as string) numero_candidato_doador,
    safe_cast(cnae_2_doador as string) cnae_2_doador,
    case
        when length(cnae_2_doador) = 5 then safe_cast(cnae_2_doador as string) else null
    end as cnae_2_doador_classe,
    case
        when length(cnae_2_doador) > 5 then safe_cast(cnae_2_doador as string) else null
    end as cnae_2_doador_subclasse,
    safe_cast(descricao_cnae_2_doador as string) descricao_cnae_2_doador,
    safe_cast(cpf_cnpj_doador_orig as string) cpf_cnpj_doador_orig,
    safe_cast(nome_doador_orig as string) nome_doador_orig,
    safe_cast(nome_doador_orig_rf as string) nome_doador_orig_rf,
    safe_cast(tipo_doador_orig as string) tipo_doador_orig,
    safe_cast(descricao_cnae_2_doador_orig as string) descricao_cnae_2_doador_orig,
    safe_cast(valor_receita as float64) valor_receita,
from {{ set_datalake_project("br_tse_eleicoes_staging.receitas_comite") }} as t
