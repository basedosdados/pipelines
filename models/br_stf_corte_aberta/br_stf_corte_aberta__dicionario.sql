{{ config(alias="dicionario", schema="br_stf_corte_aberta") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(initcap(chave) as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(initcap(valor) as string) valor,
from {{ set_datalake_project("br_stf_corte_aberta_staging.dicionario") }} as t
