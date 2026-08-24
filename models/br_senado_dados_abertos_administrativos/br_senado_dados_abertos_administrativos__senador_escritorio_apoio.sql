{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador_escritorio_apoio",
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
    safe_cast(nome_escritorio as string) nome_escritorio,
    safe_cast(sigla_setor as string) sigla_setor,
    safe_cast(endereco as string) endereco,
    safe_cast(telefone as string) telefone
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador_escritorio_apoio"
        )
    }} as t
