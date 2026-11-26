{{
    config(
        alias="operacoes_fgi_peac_cs_rs",
        schema="br_bndes_fundos_garantidores",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2023, "end": 2029, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
        labels={"project_id": "basedosdados"},
    )
}}

with
    staging as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(data_solicitacao_outorga as date) data_solicitacao_outorga,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(nome_municipio as string) nome_municipio,
            safe_cast(sigla_uf_sede_cliente as string) sigla_uf_sede_cliente,
            safe_cast(
                nome_municipio_sede_cliente as string
            ) nome_municipio_sede_cliente,
            safe_cast(cpf_cnpj_cliente as string) cnpj_cliente,
            safe_cast(nome_cliente as string) nome_cliente,
            safe_cast(porte_cliente as string) porte_cliente,
            safe_cast(valor_operacao as float64) valor_operacao,
            safe_cast(valor_garantido as float64) valor_garantido,
            safe_cast(valor_desembolsado as float64) valor_desembolsado,
            safe_cast(nome_agente_financeiro as string) nome_agente_financeiro
        from
            {{
                set_datalake_project(
                    "br_bndes_fundos_garantidores_staging.operacoes_fgi_peac_cs_rs"
                )
            }} as t
    ),

    municipio as ({{ fundos_garantidores_municipio() }})

select
    s.ano,
    s.data_solicitacao_outorga,
    s.sigla_uf,
    m.id_municipio,
    s.sigla_uf_sede_cliente,
    ms.id_municipio id_municipio_sede_cliente,
    s.cnpj_cliente,
    s.nome_cliente,
    s.porte_cliente,
    s.valor_operacao,
    s.valor_garantido,
    s.valor_desembolsado,
    s.nome_agente_financeiro
from staging as s
left join
    municipio as m
    on s.sigla_uf = m.sigla_uf
    and {{ fundos_garantidores_nome_norm("s.nome_municipio") }} = m.nome_norm
left join
    municipio as ms
    on s.sigla_uf_sede_cliente = ms.sigla_uf
    and {{ fundos_garantidores_nome_norm("s.nome_municipio_sede_cliente") }}
    = ms.nome_norm
