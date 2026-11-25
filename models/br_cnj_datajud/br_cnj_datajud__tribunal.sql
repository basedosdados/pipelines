{{
    config(
        schema="br_cnj_datajud",
        alias="tribunal",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2030, "interval": 1},
        },
    )
}}

-- Atributos do tribunal reportados por ano. Separados da tabela de indicadores
-- porque são categóricos e não caberiam na coluna numérica valor.
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_tribunal as string) sigla_tribunal,
    safe_cast(ramo_justica as string) ramo_justica,
    safe_cast(nome_tribunal as string) nome_tribunal,
    safe_cast(sigla_uf_sede as string) sigla_uf_sede,
    safe_cast(abrangencia as string) abrangencia,
    safe_cast(porte as string) porte,
    safe_cast(estrutura as string) estrutura,
    safe_cast(sequencial_orgao as string) sequencial_orgao
from {{ set_datalake_project("br_cnj_datajud_staging.tribunal") }} as t
