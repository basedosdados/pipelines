{{ config(alias="dicionario", schema="br_ibge_censo_demografico") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(replace(chave, ".0", "") as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_ibge_censo_demografico_staging.dicionario") }} as t
