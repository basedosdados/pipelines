{{
    config(
        alias="feminicidio_mensal_cisp", schema="br_rj_isp_estatisticas_seguranca"
    )
}}

with
    tabela_1 as (
        select
            safe_cast(t.ano as int64) ano,
            safe_cast(t.mes as int64) mes,
            safe_cast(t.id_cisp as string) id_cisp,
            safe_cast(t.id_aisp as string) id_aisp,
            safe_cast(t.id_risp as string) id_risp,
            safe_cast(t.id_municipio as string) municipio,
            safe_cast(
                t.quantidade_morte_feminicidio as int64
            ) quantidade_morte_feminicidio,
            safe_cast(
                quantidade_tentativa_feminicidio as int64
            ) quantidade_tentativa_feminicidio,
            safe_cast(tipo_fase as string) tipo_fase
        from
            {{
                set_datalake_project(
                    "br_rj_isp_estatisticas_seguranca_staging.feminicidio_mensal_cisp"
                )
            }} as t
    ),
    tabela_2 as (
        select *
        from `basedosdados-dev.br_bd_diretorios_brasil.municipio`
        where id_uf = '33'
    )

select
    tabela_1.ano,
    tabela_1.mes,
    tabela_1.id_cisp,
    tabela_1.id_aisp,
    tabela_1.id_risp,
    tabela_2.id_municipio,
    tabela_1.quantidade_morte_feminicidio,
    tabela_1.quantidade_tentativa_feminicidio,
    tabela_1.tipo_fase
from tabela_1 as tabela_1
left join tabela_2 as tabela_2 on tabela_1.municipio = tabela_2.nome
