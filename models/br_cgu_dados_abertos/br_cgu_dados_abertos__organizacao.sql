{{ config(alias="organizacao", schema="br_cgu_dados_abertos") }}
select
    safe_cast(nullif(o.id, "") as string) id,
    safe_cast(nullif(o.titulo, "") as string) nome,
    safe_cast(nullif(o.nome, "") as string) nome_tokenizado,
    safe_cast(nullif(o.descricao, "") as string) descricao,
    case
        when o.organizationesfera = "1"
        then "Federal"
        when o.organizationesfera = "2"
        then "Estadual/Distrital"
        when o.organizationesfera = "3"
        then "Municipal"
        else null
    end tipo_esfera_administrativa,
    safe_cast(nullif(o.organizationuf, "") as string) sigla_uf,
    safe_cast(m.id_municipio as string) id_municipio,
    safe_cast(o.qtdseguidores as int64) quantidade_seguidores,
    safe_cast(o.qtdconjuntodedados as int64) quantidade_conjuntos
from {{ set_datalake_project("br_cgu_dados_abertos_staging.organizacao") }} as o
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as m
    on o.organizationuf = m.sigla_uf
    and o.organizationmunicipio = m.nome
