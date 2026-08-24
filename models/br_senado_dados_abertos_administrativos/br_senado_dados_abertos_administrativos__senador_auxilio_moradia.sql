{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador_auxilio_moradia",
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
    safe_cast(nome_parlamentar as string) nome_parlamentar,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(indicador_auxilio_moradia as string) indicador_auxilio_moradia,
    safe_cast(indicador_imovel_funcional as string) indicador_imovel_funcional
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador_auxilio_moradia"
        )
    }} as t
