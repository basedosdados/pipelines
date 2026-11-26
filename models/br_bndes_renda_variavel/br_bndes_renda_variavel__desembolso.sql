{{
    config(
        alias="desembolso",
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
    safe_cast(tipo_capital as string) tipo_capital,
    safe_cast(tipo_ativo as string) tipo_ativo,
    safe_cast(setor_atividade as string) setor_atividade,
    safe_cast(objetivo_predominante as string) objetivo_predominante,
    safe_cast(descricao_operacao as string) descricao_operacao,
    safe_cast(valor_desembolsado as float64) valor_desembolsado
from {{ set_datalake_project("br_bndes_renda_variavel_staging.desembolso") }} as t
