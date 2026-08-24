{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador_gabinete",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(nome_parlamentar as string) nome_parlamentar,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(titular_suplente as string) titular_suplente,
    safe_cast(mandato as string) mandato,
    safe_cast(data_nascimento as date) data_nascimento,
    safe_cast(endereco as string) endereco,
    safe_cast(telefones as string) telefones,
    safe_cast(fax as string) fax,
    safe_cast(email as string) email,
    safe_cast(chefe_gabinete as string) chefe_gabinete
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador_gabinete"
        )
    }} as t
