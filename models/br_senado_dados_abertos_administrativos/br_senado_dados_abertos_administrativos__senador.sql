{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador",
        materialized="table",
    )
}}


select
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome_parlamentar as string) nome_parlamentar,
    safe_cast(nome_completo as string) nome_completo,
    safe_cast(sexo as string) sexo,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(titular_suplente as string) titular_suplente,
    safe_cast(mandato as string) mandato,
    safe_cast(data_nascimento as date) data_nascimento,
    safe_cast(email as string) email,
    safe_cast(indicador_em_exercicio as string) indicador_em_exercicio
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador"
        )
    }} as t
