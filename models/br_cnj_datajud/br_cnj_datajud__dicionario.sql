{{
    config(
        schema="br_cnj_datajud",
        alias="dicionario",
        materialized="table",
    )
}}

-- Descrição de cada código de indicador, conforme a tabela de variáveis que o
-- CNJ distribui junto com a base.
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("br_cnj_datajud_staging.dicionario") }} as t
