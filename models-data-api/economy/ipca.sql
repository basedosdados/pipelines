{{
    config(
        alias="ipca",
        schema="economy",
        materialized="table",
    )
}}

select 
    ano,
    mes,
    variacao_mensal,
    variacao_doze_meses
from basedosdados.br_ibge_ipca.mes_brasil as t