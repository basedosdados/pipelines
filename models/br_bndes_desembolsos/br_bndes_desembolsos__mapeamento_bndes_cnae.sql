{{
    config(
        alias="mapeamento_bndes_cnae",
        schema="br_bndes_desembolsos",
        materialized="table",
        labels={"project_id": "basedosdados"},
    )
}}

with
    diretorio_classe as (
        select distinct classe from basedosdados.br_bd_diretorios_brasil.cnae_2
    )

select
    safe_cast(t.cnae_2_secao as string) cnae_2_secao,
    safe_cast(t.cnae_2_divisao as string) cnae_2_divisao,
    safe_cast(t.cnae_2_grupo as string) cnae_2_grupo,
    safe_cast(coalesce(d.classe, t.cnae_2_classe) as string) cnae_2_classe,
    safe_cast(t.cnae_2_subclasse as string) cnae_2_subclasse,
    safe_cast(t.regra_produto as string) regra_produto,
    safe_cast(t.setor_cnae as string) setor_cnae,
    safe_cast(t.subsetor_cnae_agrupado as string) subsetor_cnae_agrupado,
    safe_cast(t.setor_bndes as string) setor_bndes,
    safe_cast(t.subsetor_bndes as string) subsetor_bndes
from
    {{ set_datalake_project("br_bndes_desembolsos_staging.mapeamento_bndes_cnae") }}
    as t
left join
    diretorio_classe as d
    on length(t.cnae_2_classe) = 4
    and starts_with(d.classe, t.cnae_2_classe)
