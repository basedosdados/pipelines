{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao_pagamento",
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
    safe_cast(descricao_despesa as string) descricao_despesa,
    safe_cast(valor_cobrado as float64) valor_cobrado,
    safe_cast(multa as float64) multa,
    safe_cast(glosa as float64) glosa,
    safe_cast(observacao as string) observacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao_pagamento"
        )
    }} as t
