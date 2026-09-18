{{
    config(
        alias="operacoes_exportacao_servicos",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
    )
}}

with
    staging as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(data_contratacao as date) data_contratacao,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(nome_pais_destino as string) nome_pais_destino,
            safe_cast(id_operacao as string) id_operacao,
            safe_cast(cnpj_exportador as string) cnpj_exportador,
            safe_cast(nome_exportador as string) nome_exportador,
            safe_cast(porte_exportador as string) porte_exportador,
            safe_cast(descricao_operacao as string) descricao_operacao,
            safe_cast(categoria as string) categoria,
            safe_cast(modalidade_operacional as string) modalidade_operacional,
            safe_cast(tipo_mutuario as string) tipo_mutuario,
            safe_cast(produto as string) produto,
            safe_cast(modalidade_apoio as string) modalidade_apoio,
            safe_cast(forma_apoio as string) forma_apoio,
            safe_cast(area_operacional as string) area_operacional,
            safe_cast(fonte_recurso as string) fonte_recurso,
            safe_cast(custo_financeiro as string) custo_financeiro,
            safe_cast(sigla_moeda as string) sigla_moeda,
            safe_cast(setor_bndes as string) setor_bndes,
            safe_cast(subsetor_bndes as string) subsetor_bndes,
            safe_cast(tipo_garantia as string) tipo_garantia,
            safe_cast(situacao_operacao as string) situacao_operacao,
            safe_cast(valor_operacao as float64) valor_operacao,
            safe_cast(valor_desembolsado as float64) valor_desembolsado,
            safe_cast(taxa_juros as float64) taxa_juros,
            safe_cast(prazo_meses as int64) prazo_meses
        from
            {{
                set_datalake_project(
                    "br_bndes_operacoes_contratadas_staging.operacoes_exportacao_servicos"
                )
            }}
            as t
    ),

    pais as (
        select
            sigla_iso3,
            regexp_replace(normalize(upper(nome_pt), nfd), r'\pM', '') nome_norm
        from basedosdados.br_bd_diretorios_mundo.pais
    ),

    staging_norm as (
        select
            *,
            regexp_replace(
                normalize(upper(nome_pais_destino), nfd), r'\pM', ''
            ) nome_norm
        from staging
    )

select
    s.ano,
    s.data_contratacao,
    s.sigla_uf,
    p.sigla_iso3 sigla_pais_destino,
    s.id_operacao,
    s.cnpj_exportador,
    s.nome_exportador,
    s.porte_exportador,
    s.descricao_operacao,
    s.categoria,
    s.modalidade_operacional,
    s.tipo_mutuario,
    s.produto,
    s.modalidade_apoio,
    s.forma_apoio,
    s.area_operacional,
    s.fonte_recurso,
    s.custo_financeiro,
    s.sigla_moeda,
    s.setor_bndes,
    s.subsetor_bndes,
    s.tipo_garantia,
    s.situacao_operacao,
    s.valor_operacao,
    s.valor_desembolsado,
    s.taxa_juros,
    s.prazo_meses
from staging_norm as s
left join pais as p on s.nome_norm = p.nome_norm
