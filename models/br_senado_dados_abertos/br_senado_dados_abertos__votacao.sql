{{
    config(
        alias="votacao",
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
    safe_cast(casa as string) casa,
    safe_cast(id_processo as string) id_processo,
    safe_cast(codigo_materia as string) codigo_materia,
    safe_cast(identificacao_materia as string) identificacao_materia,
    safe_cast(sigla_materia as string) sigla_materia,
    safe_cast(numero_materia as string) numero_materia,
    safe_cast(ano_materia as int64) ano_materia,
    safe_cast(id_sessao as string) id_sessao,
    safe_cast(id_sessao_legislativa as string) id_sessao_legislativa,
    safe_cast(numero_sessao as string) numero_sessao,
    safe_cast(sequencial_sessao as string) sequencial_sessao,
    safe_cast(sequencial_votacao as string) sequencial_votacao,
    safe_cast(sigla_tipo_sessao as string) sigla_tipo_sessao,
    safe_cast(descricao_votacao as string) descricao_votacao,
    safe_cast(ementa as string) ementa,
    safe_cast(resultado_votacao as string) resultado_votacao,
    safe_cast(votacao_secreta as string) votacao_secreta,
    safe_cast(data_apresentacao as date) data_apresentacao,
    safe_cast(sigla_colegiado as string) sigla_colegiado,
    safe_cast(nome_colegiado as string) nome_colegiado,
    safe_cast(voto_sim as int64) voto_sim,
    safe_cast(voto_nao as int64) voto_nao,
    safe_cast(voto_abstencao as int64) voto_abstencao,
    safe_cast(voto_outro as int64) voto_outro,
from {{ set_datalake_project("br_senado_dados_abertos_staging.votacao") }} as t
