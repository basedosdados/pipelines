{{
    config(
        alias="licitacao",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Processos licitatórios dos governos estaduais. Uma linha por processo de contratação.
-- Ligam-se à execução por `relacionamentos`, quando a fonte publica o vínculo.
select *
from {{ ref("br_bd_execucao_estadual__licitacao_mg") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__licitacao_ba") }}
