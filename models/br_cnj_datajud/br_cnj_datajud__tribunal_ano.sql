{{
    config(
        schema="br_cnj_datajud",
        alias="tribunal_ano",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2030, "interval": 1},
        },
    )
}}

-- Painel Justiça em Números em formato longo: uma linha por tribunal, ano e
-- indicador. A fonte publica 1.305 indicadores em colunas separadas; o formato
-- longo mantém a tabela estável quando o CNJ acrescenta indicadores.
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_tribunal as string) sigla_tribunal,
    safe_cast(ramo_justica as string) ramo_justica,
    safe_cast(sigla_indicador as string) sigla_indicador,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_cnj_datajud_staging.tribunal_ano") }} as t
