{{
    config(
        alias="dicionario", schema="br_cgu_cartao_pagamento", materialized="table"
    )
}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(trim(chave) as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_cgu_cartao_pagamento_staging.dicionario") }} as t
