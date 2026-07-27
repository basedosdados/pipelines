{{
    config(
        alias="operacoes_administracao_publica",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
        labels={"project_id": "basedosdados"},
    )
}}

with
    staging as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(nome_municipio as string) nome_municipio,
            safe_cast(ente_publico as string) ente_publico,
            safe_cast(programa as string) programa,
            safe_cast(modalidade_operacional as string) modalidade_operacional,
            safe_cast(data_nivel_atual as date) data_nivel_atual,
            safe_cast(nivel_atual as string) nivel_atual,
            safe_cast(situacao_operacao as string) situacao_operacao,
            safe_cast(descricao_projeto as string) descricao_projeto,
            safe_cast(valor_operacao as float64) valor_operacao,
            safe_cast(valor_desembolsado as float64) valor_desembolsado,
            safe_cast(valor_saldo_liberar as float64) valor_saldo_liberar
        from
            {{
                set_datalake_project(
                    "br_bndes_operacoes_contratadas_staging.operacoes_administracao_publica"
                )
            }}
            as t
    ),

    municipio as (
        select
            id_municipio,
            sigla_uf,
            regexp_replace(normalize(upper(nome), nfd), r'\pM', '') nome_norm
        from {{ ref("br_bd_diretorios_brasil__municipio") }}
    ),

    staging_norm as (
        select
            *,
            regexp_replace(
                normalize(
                    upper(
                        case
                            nome_municipio
                            when 'BRASOPOLIS'
                            then 'Brazópolis'
                            when 'MOGI-GUACU'
                            then 'Mogi Guaçu'
                            when 'ITABIRINHA DE MANTENA'
                            then 'Itabirinha'
                            when 'ALTO ALEGRE DO PARECIS'
                            then 'Alto Alegre dos Parecis'
                            when 'BALNEARIO DE PICARRAS'
                            then 'Balneário Piçarras'
                            when 'PRESIDENTE CASTELO BRANCO'
                            then 'Presidente Castello Branco'
                            when 'SAO TOME DAS LETRAS'
                            then 'São Thomé das Letras'
                            when 'COUTO DE MAGALHAES'
                            then 'Couto Magalhães'
                            else nome_municipio
                        end
                    ),
                    nfd
                ),
                r'\pM',
                ''
            ) nome_norm
        from staging
    )

select
    s.ano,
    s.sigla_uf,
    m.id_municipio,
    s.ente_publico,
    s.programa,
    s.modalidade_operacional,
    s.data_nivel_atual,
    s.nivel_atual,
    s.situacao_operacao,
    s.descricao_projeto,
    s.valor_operacao,
    s.valor_desembolsado,
    s.valor_saldo_liberar
from staging_norm as s
left join municipio as m on s.sigla_uf = m.sigla_uf and s.nome_norm = m.nome_norm
