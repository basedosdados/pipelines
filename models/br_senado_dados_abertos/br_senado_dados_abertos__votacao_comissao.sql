{{
    config(
        alias="votacao_comissao",
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
    safe_cast(id_comissao as string) id_comissao,
    safe_cast(sigla_colegiado as string) sigla_colegiado,
    safe_cast(nome_colegiado as string) nome_colegiado,
    safe_cast(sigla_casa_colegiado as string) sigla_casa_colegiado,
    safe_cast(id_reuniao as string) id_reuniao,
    safe_cast(numero_reuniao as string) numero_reuniao,
    safe_cast(tipo_reuniao as string) tipo_reuniao,
    safe_cast(data_reuniao as date) data_reuniao,
    safe_cast(identificacao_materia as string) identificacao_materia,
    safe_cast(sigla_materia as string) sigla_materia,
    safe_cast(numero_materia as string) numero_materia,
    safe_cast(ano_materia as int64) ano_materia,
    safe_cast(descricao as string) descricao,
    safe_cast(id_senador_presidente as string) id_senador_presidente,
    safe_cast(voto_sim as int64) voto_sim,
    safe_cast(voto_nao as int64) voto_nao,
    safe_cast(voto_abstencao as int64) voto_abstencao,
from {{ set_datalake_project("br_senado_dados_abertos_staging.votacao_comissao") }} as t
