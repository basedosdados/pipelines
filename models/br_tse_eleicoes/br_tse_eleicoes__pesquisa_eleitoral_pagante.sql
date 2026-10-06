{{
    config(
        schema="br_tse_eleicoes",
        alias="pesquisa_eleitoral_pagante",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2012, "end": 2030, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_pesquisa as string) id_pesquisa,
    safe_cast(id_contratante as string) id_contratante,
    safe_cast(cpf_cnpj_pagante as string) cpf_cnpj_pagante,
    safe_cast(nome_pagante as string) nome_pagante,
    safe_cast(origem_recurso as string) origem_recurso
from
    {{ set_datalake_project("br_tse_eleicoes_staging.pesquisa_eleitoral_pagante") }}
    as t
