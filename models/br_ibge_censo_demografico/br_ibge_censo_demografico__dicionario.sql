{{ config(alias="dicionario", schema="br_ibge_censo_demografico") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(replace(chave, ".0", "") as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from basedosdados.br_ibge_censo_demografico.dicionario as hist
where hist.id_tabela not like '%_2022'
union all
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(replace(chave, ".0", "") as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_ibge_censo_demografico_staging.dicionario") }} as t
where t.id_tabela like '%_2022'
