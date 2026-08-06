{{
    config(
        alias="votacao_parlamentar",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_votacao as string) id_votacao,
    safe_cast(data_sessao as date) data_sessao,
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome as string) nome,
    safe_cast(sexo as string) sexo,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(voto as string) voto,
    safe_cast(descricao_voto as string) descricao_voto,
from
    {{ set_datalake_project("br_senado_dados_abertos_staging.votacao_parlamentar") }}
    as t
