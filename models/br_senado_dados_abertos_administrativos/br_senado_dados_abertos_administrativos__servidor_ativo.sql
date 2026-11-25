{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_ativo",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(nome as string) nome,
    safe_cast(tipo_vinculo as string) tipo_vinculo,
    safe_cast(cargo as string) cargo,
    safe_cast(categoria as string) categoria,
    safe_cast(funcao as string) funcao,
    safe_cast(data_admissao as date) data_admissao,
    safe_cast(jornada_semanal_horas as int64) jornada_semanal_horas,
    safe_cast(afastamento as string) afastamento,
    safe_cast(isencao_ponto as string) isencao_ponto,
    safe_cast(sigla_lotacao as string) sigla_lotacao,
    safe_cast(lotacao as string) lotacao,
    safe_cast(nivel_lotacao as int64) nivel_lotacao,
    safe_cast(hierarquia_lotacao as string) hierarquia_lotacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_ativo"
        )
    }} as t
