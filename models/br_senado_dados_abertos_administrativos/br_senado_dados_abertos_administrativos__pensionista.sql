{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="pensionista",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(id_pensionista as string) id_pensionista,
    safe_cast(nome as string) nome,
    safe_cast(vinculo as string) vinculo,
    safe_cast(fundamento as string) fundamento,
    safe_cast(nome_instituidor as string) nome_instituidor,
    safe_cast(codigo_categoria as string) codigo_categoria,
    safe_cast(categoria as string) categoria,
    safe_cast(cargo as string) cargo,
    safe_cast(codigo_funcao as string) codigo_funcao,
    safe_cast(funcao as string) funcao,
    safe_cast(ano_exercicio as int64) ano_exercicio,
    safe_cast(data_obito as date) data_obito,
    safe_cast(data_inicio_pensao as date) data_inicio_pensao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.pensionista"
        )
    }} as t
