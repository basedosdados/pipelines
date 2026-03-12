{{ config(alias="dicionario", schema="br_bcb_sicor") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(ltrim(chave, '0') as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_bcb_sicor_staging.dicionario") }} as t
union all
{{
    dicionario_not_found(
        id_tabela="empreendimento", nome_coluna="id_tipo_cultura", chave="4"
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="microdados_recurso_publico_mutuario",
        nome_coluna="tipo_beneficiario",
        chave=["14", "15", "16", "17", "18"],
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="microdados_operacoes_desclassificadas",
        nome_coluna="id_motivo_desclassificacao",
        chave=["0", "201", "14"],
    )
}}
