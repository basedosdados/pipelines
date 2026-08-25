{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="senador_gabinete",
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
    safe_cast(id_senador as string) id_senador,
    safe_cast(endereco as string) endereco,
    safe_cast(telefones as string) telefones,
    safe_cast(fax as string) fax,
    safe_cast(chefe_gabinete as string) chefe_gabinete
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.senador_gabinete"
        )
    }} as t
