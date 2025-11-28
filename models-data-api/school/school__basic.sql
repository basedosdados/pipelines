{{
    config(
        alias="basic",
        schema="school",
        materialized="table",
    )
}}
select
    safe_cast(id_escola as string) id_escola,
    safe_cast(nome as string) nome,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(endereco as string) endereco,
    safe_cast(categoria_administrativa as string) categoria_administrativa,
    safe_cast(dependencia_administrativa as string) dependencia_administrativa,
    safe_cast(etapas_modalidades_oferecidas as string) etapas_modalidades_oferecidas,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
from basedosdados.br_bd_diretorios_brasil.escola as t
order by id_escola asc
