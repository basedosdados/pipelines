{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="diretor_coordenador",
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
    safe_cast(sigla_setor as string) sigla_setor,
    safe_cast(setor as string) setor,
    safe_cast(sigla_setor_superior as string) sigla_setor_superior,
    safe_cast(setor_superior as string) setor_superior,
    safe_cast(matricula_titular as string) matricula_titular,
    safe_cast(nome_titular as string) nome_titular,
    safe_cast(cargo_titular as string) cargo_titular,
    safe_cast(email_titular as string) email_titular,
    safe_cast(referencia_chefia as string) referencia_chefia,
    safe_cast(matricula_substituto as string) matricula_substituto,
    safe_cast(nome_substituto as string) nome_substituto,
    safe_cast(data_inicio_substituicao as date) data_inicio_substituicao,
    safe_cast(data_fim_substituicao as date) data_fim_substituicao,
    safe_cast(telefone as string) telefone,
    safe_cast(endereco as string) endereco
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.diretor_coordenador"
        )
    }} as t
