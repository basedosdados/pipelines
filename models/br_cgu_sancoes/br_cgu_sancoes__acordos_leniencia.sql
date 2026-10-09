{{
    config(
        schema="br_cgu_sancoes",
        alias="acordos_leniencia",
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
    safe_cast(id_acordo as string) id_acordo,
    safe_cast(cnpj_sancionado as string) cnpj_sancionado,
    safe_cast(razao_social_receita as string) razao_social_receita,
    safe_cast(nome_fantasia_receita as string) nome_fantasia_receita,
    safe_cast(data_inicio_acordo as date) data_inicio_acordo,
    safe_cast(data_fim_acordo as date) data_fim_acordo,
    safe_cast(situacao_acordo as string) situacao_acordo,
    safe_cast(data_informacao as date) data_informacao,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(termos_acordo as string) termos_acordo,
    safe_cast(orgao_sancionador as string) orgao_sancionador
from {{ set_datalake_project("br_cgu_sancoes_staging.acordos_leniencia") }} as t
