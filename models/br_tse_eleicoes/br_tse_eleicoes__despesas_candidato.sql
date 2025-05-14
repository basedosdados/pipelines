{{
    config(
        schema="br_tse_eleicoes",
        alias="despesas_candidato",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2024, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(turno as int64) turno,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(data_eleicao as date) data_eleicao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(titulo_eleitoral_candidato as string) titulo_eleitoral_candidato,
    safe_cast(sequencial_candidato as string) sequencial_candidato,
    safe_cast(numero_candidato as string) numero_candidato,
    safe_cast(cnpj_candidato as string) cnpj_candidato,
    safe_cast(numero_partido as string) numero_partido,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(cargo as string) cargo,
    safe_cast(sequencial_despesa as string) sequencial_despesa,
    safe_cast(data_despesa as date) data_despesa,
    safe_cast(tipo_despesa as string) tipo_despesa,
    safe_cast(descricao_despesa as string) descricao_despesa,
    safe_cast(origem_despesa as string) origem_despesa,
    safe_cast(valor_despesa as float64) valor_despesa,
    safe_cast(tipo_prestacao_contas as string) tipo_prestacao_contas,
    safe_cast(data_prestacao_contas as date) data_prestacao_contas,
    safe_cast(sequencial_prestador_contas as string) sequencial_prestador_contas,
    safe_cast(cnpj_prestador_contas as string) cnpj_prestador_contas,
    safe_cast(tipo_documento as string) tipo_documento,
    safe_cast(numero_documento as string) numero_documento,
    safe_cast(especie_recurso as string) especie_recurso,
    safe_cast(fonte_recurso as string) fonte_recurso,
    safe_cast(cpf_cnpj_fornecedor as string) cpf_cnpj_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(nome_fornecedor_rf as string) nome_fornecedor_rf,
    safe_cast(cnae_2_fornecedor as string) cnae_2_fornecedor,
    case
        when length(cnae_2_fornecedor) = 5
        then safe_cast(cnae_2_fornecedor as string)
        else null
    end as cnae_2_fornecedor_classe,
    case
        when length(cnae_2_fornecedor) = 7
        then safe_cast(cnae_2_fornecedor as string)
        else null
    end as cnae_2_fornecedor_subclasse,
    safe_cast(descricao_cnae_2_fornecedor as string) descricao_cnae_2_fornecedor,
    safe_cast(tipo_fornecedor as string) tipo_fornecedor,
    safe_cast(esfera_partidaria_fornecedor as string) esfera_partidaria_fornecedor,
    safe_cast(sigla_uf_fornecedor as string) sigla_uf_fornecedor,
    safe_cast(id_municipio_tse_fornecedor as string) id_municipio_tse_fornecedor,
    safe_cast(
        sequencial_candidato_fornecedor as string
    ) sequencial_candidato_fornecedor,
    safe_cast(numero_candidato_fornecedor as string) numero_candidato_fornecedor,
    safe_cast(numero_partido_fornecedor as string) numero_partido_fornecedor,
    safe_cast(sigla_partido_fornecedor as string) sigla_partido_fornecedor,
    safe_cast(cargo_fornecedor as string) cargo_fornecedor
from {{ set_datalake_project("br_tse_eleicoes_staging.despesas_candidato") }} as t
