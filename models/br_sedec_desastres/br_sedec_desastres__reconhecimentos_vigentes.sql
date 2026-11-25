-- Último retrato promovido: 2026-09-02
--
-- Esta linha é o gatilho da promoção: a action table-approve só age sobre
-- arquivos .sql alterados na PR. Ao subir um retrato novo, bumpar a data.
{{
    config(
        schema="br_sedec_desastres",
        alias="reconhecimentos_vigentes",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}
with
    staging as (
        select
            safe_cast(data_extracao as date) as data_extracao,
            safe_cast(sigla_uf as string) as sigla_uf,
            safe_cast(nome_municipio as string) as nome_municipio,
            safe_cast(id_cobrade as string) as id_cobrade,
            safe_cast(nome_cobrade as string) as nome_cobrade,
            safe_cast(situacao as string) as situacao,
            safe_cast(data_ocorrencia as date) as data_ocorrencia,
            safe_cast(data_vigencia as date) as data_vigencia
        from
            {{
                set_datalake_project(
                    "br_sedec_desastres_staging.reconhecimentos_vigentes"
                )
            }}
    ),
    municipio as (
        select
            id_municipio,
            sigla_uf,
            regexp_replace(
                normalize(upper(replace(nome, '-', ' ')), nfd), r'[^A-Z0-9 ]', ''
            ) as nome_norm
        from `basedosdados.br_bd_diretorios_brasil.municipio`
    ),
    staging_norm as (
        select
            *,
            regexp_replace(
                normalize(
                    upper(
                        replace(
                            case
                                when
                                    sigla_uf = 'BA'
                                    and nome_municipio = 'Muquém do São Francisco'
                                then 'Muquém de São Francisco'
                                when sigla_uf = 'CE' and nome_municipio = 'Itapajé'
                                then 'Itapagé'
                                when sigla_uf = 'MT' and nome_municipio = 'Poxoréu'
                                then 'Poxoréo'
                                when
                                    sigla_uf = 'PA'
                                    and nome_municipio = 'Santa Izabel do Pará'
                                then 'Santa Isabel do Pará'
                                when sigla_uf = 'PE' and nome_municipio = 'Iguaracy'
                                then 'Iguaraci'
                                when sigla_uf = 'PE' and nome_municipio = 'São Caetano'
                                then 'São Caitano'
                                else nome_municipio
                            end,
                            '-',
                            ' '
                        )
                    ),
                    nfd
                ),
                r'[^A-Z0-9 ]',
                ''
            ) as nome_norm
        from staging
    )
select
    s.data_extracao,
    s.sigla_uf,
    m.id_municipio,
    s.id_cobrade,
    s.nome_cobrade,
    s.situacao,
    s.data_ocorrencia,
    s.data_vigencia
from staging_norm as s
left join municipio as m on s.sigla_uf = m.sigla_uf and s.nome_norm = m.nome_norm
