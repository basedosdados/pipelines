{{
    config(
        schema="br_rf_arrecadacao",
        alias="natureza_juridica",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}

with
    referencia_codigo as (
        select
            id_natureza_juridica,
            substr(cast(id_natureza_juridica as string), 0, 3) as inicio_codigo
        from basedosdados.br_bd_diretorios_brasil.natureza_juridica
    )
select
    safe_cast(t.ano as int64) ano,
    safe_cast(t.mes as int64) mes,
    safe_cast(
        referencia_codigo.id_natureza_juridica as string
    ) natureza_juridica_codigo,
    safe_cast(t.imposto_importacao as float64) imposto_importacao,
    safe_cast(t.imposto_exportacao as float64) imposto_exportacao,
    safe_cast(t.ipi as float64) ipi,
    safe_cast(t.irpf as float64) irpf,
    safe_cast(t.irpj as float64) irpj,
    safe_cast(t.irrf as float64) irrf,
    safe_cast(t.iof as float64) iof,
    safe_cast(t.itr as float64) itr,
    safe_cast(t.cofins as float64) cofins,
    safe_cast(t.pis_pasep as float64) pis_pasep,
    safe_cast(t.csll as float64) csll,
    safe_cast(t.cide_combustiveis as float64) cide_combustiveis,
    safe_cast(t.contribuicao_previdenciaria as float64) contribuicao_previdenciaria,
    safe_cast(t.cpsss as float64) cpsss,
    safe_cast(t.pagamento_unificado as float64) pagamento_unificado,
    safe_cast(t.outras_receitas_rfb as float64) outras_receitas_rfb,
    safe_cast(t.demais_receitas as float64) demais_receitas,
from {{ set_datalake_project("br_rf_arrecadacao_staging.natureza_juridica") }} as t
inner join
    referencia_codigo on t.natureza_juridica_codigo = referencia_codigo.inicio_codigo
