{{
    config(
        alias="votacao_comissao_parlamentar",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_votacao as string) id_votacao,
    safe_cast(data_reuniao as date) data_reuniao,
    safe_cast(id_senador as string) id_senador,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_casa as string) sigla_casa,
    safe_cast(voto as string) voto,
    safe_cast(voto_presidente as string) voto_presidente,
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_staging.votacao_comissao_parlamentar"
        )
    }} as t
