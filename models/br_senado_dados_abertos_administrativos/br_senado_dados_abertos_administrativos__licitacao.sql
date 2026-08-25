{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="licitacao",
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
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(numero as string) numero,
    safe_cast(modalidade as string) modalidade,
    safe_cast(situacao as string) situacao,
    safe_cast(objeto as string) objeto,
    safe_cast(data_abertura as date) data_abertura,
    safe_cast(indicador_registro_preco as string) indicador_registro_preco,
    safe_cast(orgao_origem as string) orgao_origem,
    safe_cast(edital as string) edital
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.licitacao"
        )
    }} as t
