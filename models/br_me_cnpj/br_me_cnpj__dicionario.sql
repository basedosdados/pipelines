{{ config(alias="dicionario", schema="br_me_cnpj") }}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(initcap(valor) as string) valor,
from {{ set_datalake_project("br_me_cnpj_staging.dicionario") }} as t
