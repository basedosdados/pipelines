{{
    config(
        schema="br_rf_arrecadacao",
        alias="cnae",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(secao_sigla as string) secao_sigla,
    safe_cast(imposto_importacao as float64) imposto_importacao,
    safe_cast(imposto_exportacao as float64) imposto_exportacao,
    safe_cast(ipi as float64) ipi,
    safe_cast(irpf as float64) irpf,
    safe_cast(irpj as float64) irpj,
    safe_cast(irrf as float64) irrf,
    safe_cast(iof as float64) iof,
    safe_cast(itr as float64) itr,
    safe_cast(cofins as float64) cofins,
    safe_cast(pis_pasep as float64) pis_pasep,
    safe_cast(csll as float64) csll,
    safe_cast(cide_combustiveis as float64) cide_combustiveis,
    safe_cast(contribuicao_previdenciaria as float64) contribuicao_previdenciaria,
    safe_cast(cpsss as float64) cpsss,
    safe_cast(pagamento_unificado as float64) pagamento_unificado,
    safe_cast(outras_receitas_rfb as float64) outras_receitas_rfb,
    safe_cast(demais_receitas as float64) demais_receitas,
from {{ set_datalake_project("br_rf_arrecadacao_staging.cnae") }} as t
