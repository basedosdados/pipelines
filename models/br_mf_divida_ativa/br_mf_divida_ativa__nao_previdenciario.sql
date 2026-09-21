-- Fonte: PGFN Dados Abertos da Divida Ativa da Uniao (SIDA, nao previdenciario)
{{
    config(
        schema="br_mf_divida_ativa",
        alias="nao_previdenciario",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2035, "interval": 1},
        },
        cluster_by=["trimestre", "sigla_uf"],
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(trimestre as int64) trimestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(cpf_cnpj as string) cpf_cnpj,
    safe_cast(tipo_pessoa as string) tipo_pessoa,
    safe_cast(tipo_devedor as string) tipo_devedor,
    safe_cast(nome_devedor as string) nome_devedor,
    safe_cast(unidade_responsavel as string) unidade_responsavel,
    safe_cast(numero_inscricao as string) numero_inscricao,
    safe_cast(tipo_situacao_inscricao as string) tipo_situacao_inscricao,
    safe_cast(situacao_inscricao as string) situacao_inscricao,
    safe_cast(receita_principal as string) receita_principal,
    safe_cast(data_inscricao as date) data_inscricao,
    safe_cast(indicador_ajuizado as string) indicador_ajuizado,
    safe_cast(valor_consolidado as float64) valor_consolidado
from {{ set_datalake_project("br_mf_divida_ativa_staging.nao_previdenciario") }} as t
