{{
    config(
        schema="br_fnde_fundeb",
        alias="indicador_municipal",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}


-- A fonte publica o código do município com 6 dígitos, sem o verificador; o
-- código IBGE completo vem do diretório.
select
    safe_cast(t.ano as int64) ano,
    safe_cast(t.bimestre as int64) bimestre,
    safe_cast(t.sigla_uf as string) sigla_uf,
    safe_cast(bd.id_municipio as string) id_municipio,
    safe_cast(t.id_indicador as string) id_indicador,
    safe_cast(t.codigo_indicador as string) codigo_indicador,
    safe_cast(t.valor_percentual as float64) valor_percentual,
    safe_cast(t.valor_real as float64) valor_real
from {{ set_datalake_project("br_fnde_fundeb_staging.indicador_municipal") }} as t
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as bd
    on t.id_municipio = bd.id_municipio_6
