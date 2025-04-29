{{
    config(
        schema="br_tse_eleicoes",
        alias="bens_candidato",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2006, "end": 2024, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(data_eleicao as date) data_eleicao,
    safe_cast(titulo_eleitoral_candidato as string) titulo_eleitoral_candidato,
    safe_cast(sequencial_candidato as string) sequencial_candidato,
    safe_cast(tipo_item as string) tipo_item,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(valor_item as float64) valor_item
from {{ set_datalake_project("br_tse_eleicoes_staging.bens_candidato") }} as t
