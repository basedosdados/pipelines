{{
    config(
        alias="escola",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_escola as string) id_escola,
    safe_cast(nome as string) nome,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(restricao_atendimento as string) restricao_atendimento,
    safe_cast(localizacao as string) localizacao,
    safe_cast(localidade_diferenciada as string) localidade_diferenciada,
    safe_cast(categoria_administrativa as string) categoria_administrativa,
    safe_cast(endereco as string) endereco,
    safe_cast(telefone as string) telefone,
    safe_cast(dependencia_administrativa as string) dependencia_administrativa,
    safe_cast(categoria_privada as string) categoria_privada,
    safe_cast(conveniada_poder_publico as string) conveniada_poder_publico,
    safe_cast(regulacao_conselho_educacao as string) regulacao_conselho_educacao,
    safe_cast(porte as string) porte,
    safe_cast(etapas_modalidades_oferecidas as string) etapas_modalidades_oferecidas,
    safe_cast(outras_ofertas_educacionais as string) outras_ofertas_educacionais,
    safe_cast(latitude as string) latitude,
    safe_cast(longitude as string) longitude
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.escola") }} as t
