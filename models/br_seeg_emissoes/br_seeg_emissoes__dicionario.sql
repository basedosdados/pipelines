{{
    config(
        alias="dicionario",
        schema="br_seeg_emissoes",
        materialized="table",
    )
}}

select
    safe_cast(tabela as string) tabela,
    safe_cast(coluna as string) coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_seeg_emissoes_staging.dicionario") }} as t
