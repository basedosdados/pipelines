{{
    config(
        alias="empresa_participacao_acionaria",
        schema="br_bndes_renda_variavel",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2006, "end": 2031, "interval": 1},
        },
        labels={"project_id": "basedosdados"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(cnpj as string) cnpj,
    safe_cast(sigla as string) sigla,
    safe_cast(razao_social as string) razao_social,
    safe_cast(tipo_ativo as string) tipo_ativo,
    safe_cast(setor_atividade as string) setor_atividade,
    safe_cast(tipo_capital as string) tipo_capital,
    safe_cast(indicador_acordo_acionistas as int64) indicador_acordo_acionistas,
    safe_cast(
        indicador_conselho_administracao as int64
    ) indicador_conselho_administracao,
    safe_cast(indicador_conselho_fiscal as int64) indicador_conselho_fiscal,
    safe_cast(proporcao_participacao_on as float64) proporcao_participacao_on,
    safe_cast(proporcao_participacao_pn as float64) proporcao_participacao_pn,
    safe_cast(proporcao_participacao_total as float64) proporcao_participacao_total
from
    {{
        set_datalake_project(
            "br_bndes_renda_variavel_staging.empresa_participacao_acionaria"
        )
    }} as t
