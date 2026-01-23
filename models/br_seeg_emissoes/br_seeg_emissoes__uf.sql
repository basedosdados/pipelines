{{
    config(
        schema="br_seeg_emissoes",
        alias="uf",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2024, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(bioma as string) bioma,
    safe_cast(gas as string) gas,
    safe_cast(tipo as string) tipo,
    safe_cast(recorte as string) recorte,
    safe_cast(setor as string) setor,
    safe_cast(atividade_economica as string) atividade_economica,
    safe_cast(categoria as string) categoria,
    safe_cast(subcategoria as string) subcategoria,
    safe_cast(produto as string) produto,
    safe_cast(detalhamento as string) detalhamento,
    safe_cast(emissao as float64) emissao,
from {{ set_datalake_project("br_seeg_emissoes_staging.uf") }} as t
