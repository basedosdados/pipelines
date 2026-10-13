{{
    config(
        alias="dicionario",
        schema="us_ssa_beneficiaries",
        materialized="table",
    )
}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
    safe_cast(valor_en as string) valor_en,
    safe_cast(valor_es as string) valor_es
from {{ set_datalake_project("us_ssa_beneficiaries_staging.dicionario") }} as t
