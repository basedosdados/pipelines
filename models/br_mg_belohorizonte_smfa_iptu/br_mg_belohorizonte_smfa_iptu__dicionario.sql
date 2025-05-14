{{ config(alias="dicionario", schema="br_mg_belohorizonte_smfa_iptu") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    replace(
        replace(
            valor,
            '(Zona de Especial Interesse Social - 2',
            'Zona de Especial Interesse Social - 2'
        ),
        '(Zona de Preservação Ambiental',
        'Zona de Preservação Ambiental'
    ) as valor
from {{ set_datalake_project("br_mg_belohorizonte_smfa_iptu_staging.dicionario") }} as t
