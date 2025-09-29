{{
    config(
        alias="mes_categoria_municipio",
        schema="br_ibge_ipca15",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    municipio.id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_categoria as string) id_categoria,
    safe_cast(categoria as string) categoria,
    safe_cast(peso_mensal as float64) peso_mensal,
    safe_cast(variacao_mensal as float64) variacao_mensal,
    safe_cast(variacao_anual as float64) variacao_anual,
    safe_cast(variacao_doze_meses as float64) variacao_doze_meses
from {{ set_datalake_project("br_ibge_ipca15_staging.mes_categoria_municipio") }} as t

left join
    basedosdados - dev.br_bd_diretorios_brasil.municipio as municipio
    on t.municipio = municipio.nome
