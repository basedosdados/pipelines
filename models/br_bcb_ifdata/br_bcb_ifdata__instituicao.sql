{{
    config(
        alias="instituicao",
        schema="br_bcb_ifdata",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2031, "interval": 1},
        },
        cluster_by=["mes", "tipo_consolidado"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(tipo_consolidado as string) tipo_consolidado,
    safe_cast(id_instituicao as string) id_instituicao,
    safe_cast(nome_instituicao as string) nome_instituicao,
    safe_cast(tcb as string) tcb,
    safe_cast(td as string) td,
    safe_cast(tc as string) tc,
    safe_cast(ti as string) ti,
    safe_cast(sr as string) sr,
    safe_cast(segmento as string) segmento,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_conglomerado_financeiro as string) id_conglomerado_financeiro,
    safe_cast(id_conglomerado_prudencial as string) id_conglomerado_prudencial,
    safe_cast(data_alteracao_segmento as date) data_alteracao_segmento
from {{ set_datalake_project("br_bcb_ifdata_staging.instituicao") }} as t
