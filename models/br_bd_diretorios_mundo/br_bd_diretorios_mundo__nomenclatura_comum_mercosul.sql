{{ config(alias="nomenclatura_comum_mercosul", schema="br_bd_diretorios_mundo") }}


select
    safe_cast(co_ncm as string) id_ncm,
    safe_cast(co_unid as string) id_unidade,
    safe_cast(co_sh6 as string) id_sh6,
    safe_cast(co_ppe as string) id_ppe,
    safe_cast(co_ppi as string) id_ppi,
    safe_cast(co_fat_agreg as string) id_fator_agregado_ncm,
    safe_cast(co_cgce_n3 as string) id_cgce_n3,
    safe_cast(co_isic_classe as string) id_isic_classe,
    safe_cast(co_siit as string) id_siit,
    safe_cast(co_cuci_item as string) id_cuci_item,
    safe_cast(
        case
            when co_unid = '11'
            then 'número (unidade)'
            when co_unid = '10'
            then 'quilograma líquido'
            when co_unid = '12'
            then 'milheiro'
            when co_unid = '14'
            then 'metro'
            when co_unid = '13'
            then 'pares'
            when co_unid = '15'
            then 'metro quadrado'
            when co_unid = '16'
            then 'metro cúbico'
            when co_unid = '17'
            then 'litro'
            when co_unid = '18'
            then 'mil quilowatt hora'
            when co_unid = '19'
            then 'quilate'
            when co_unid = '20'
            then 'duzia'
            when co_unid = '21'
            then 'tonelada métrica líquida'
            when co_unid = '22'
            then 'grama líquido'
            when co_unid = '23'
            then 'bilhões de unidades internacionais'
            when co_unid = '24'
            then 'quilograma bruto'
            else co_unid
        end as string
    ) as nome_unidade,
    safe_cast(no_ncm_por as string) nome_ncm_portugues,
    safe_cast(no_ncm_esp as string) nome_ncm_espanhol,
    safe_cast(no_ncm_ing as string) nome_ncm_ingles,
from
    `basedosdados-staging.br_bd_diretorios_mundo_staging.nomenclatura_comum_mercosul`
    as d
