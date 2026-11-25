{{
    config(
        schema="au_ato_taxation_statistics",
        alias="dicionario",
        materialized="table",
    )
}}


select
    safe_cast(t.id_tabela as string) id_tabela,
    safe_cast(t.nome_coluna as string) nome_coluna,
    safe_cast(t.chave as string) chave,
    safe_cast(t.cobertura_temporal as string) cobertura_temporal,
    safe_cast(t.valor as string) valor
from {{ set_datalake_project("au_ato_taxation_statistics_staging.dicionario") }} as t
