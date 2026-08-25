{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor",
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
    safe_cast(id_servidor as string) id_servidor,
    safe_cast(nome as string) nome,
    safe_cast(vinculo as string) vinculo,
    safe_cast(situacao as string) situacao,
    safe_cast(cargo as string) cargo,
    safe_cast(especialidade as string) especialidade,
    safe_cast(padrao as string) padrao,
    safe_cast(codigo_categoria as string) codigo_categoria,
    safe_cast(categoria as string) categoria,
    safe_cast(codigo_funcao as string) codigo_funcao,
    safe_cast(funcao as string) funcao,
    safe_cast(sigla_lotacao as string) sigla_lotacao,
    safe_cast(lotacao as string) lotacao,
    safe_cast(tipo_cessao as string) tipo_cessao,
    safe_cast(orgao_origem as string) orgao_origem,
    safe_cast(orgao_destino as string) orgao_destino,
    safe_cast(ano_admissao as int64) ano_admissao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor"
        )
    }} as t
