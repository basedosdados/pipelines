{{ config(alias="dicionario", schema="br_me_caged") }}
select
    safe_cast(
        replace(
            id_tabela, "microdados_movimentacoes", "microdados_movimentacao"
        ) as string
    ) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_me_caged_staging.dicionario") }} as t
