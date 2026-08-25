{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao_pagamento_empenho",
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
    safe_cast(tipo_contratacao as string) tipo_contratacao,
    safe_cast(id_contratacao as string) id_contratacao,
    safe_cast(id_pagamento as string) id_pagamento,
    safe_cast(id_empenho as string) id_empenho,
    safe_cast(natureza_despesa as string) natureza_despesa,
    safe_cast(valor_empenhado as float64) valor_empenhado,
    safe_cast(valor_liquidado as float64) valor_liquidado,
    safe_cast(saldo as float64) saldo
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao_pagamento_empenho"
        )
    }}
    as t
