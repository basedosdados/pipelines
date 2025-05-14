{{ config(alias="cnae_2", schema="br_bd_diretorios_brasil") }}
select
    safe_cast(subclasse as string) subclasse,
    safe_cast(descricao_subclasse as string) descricao_subclasse,
    safe_cast(classe as string) classe,
    safe_cast(descricao_classe as string) descricao_classe,
    safe_cast(grupo as string) grupo,
    safe_cast(descricao_grupo as string) descricao_grupo,
    safe_cast(divisao as string) divisao,
    safe_cast(descricao_divisao as string) descricao_divisao,
    safe_cast(secao as string) secao,
    safe_cast(descricao_secao as string) descricao_secao,
    safe_cast(indicador_cnae_2_0 as int64) indicador_cnae_2_0,
    safe_cast(indicador_cnae_2_1 as int64) indicador_cnae_2_1,
    safe_cast(indicador_cnae_2_2 as int64) indicador_cnae_2_2,
    safe_cast(indicador_cnae_2_3 as int64) indicador_cnae_2_3,
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cnae_2") }} as t
