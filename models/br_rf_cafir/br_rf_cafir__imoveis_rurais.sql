{{
    config(
        schema="br_rf_cafir",
        alias="imoveis_rurais",
        materialized="incremental",
        partition_by={
            "field": "data_referencia",
            "data_type": "date",
        },
        cluster_by=["sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}

with
    lower_munis as (
        select *, lower(municipio) as nome_mun,
        from {{ set_datalake_project("br_rf_cafir_staging.imoveis_rurais") }}
    ),
    fixed_names as (
        select
            case
                when nome_mun = 'lagoa do itaenga'
                then 'lagoa de itaenga'
                when nome_mun = 'itapaje'
                then 'itapage'
                when nome_mun = "olho d'agua do borges"
                then "olho-d'agua do borges"
                when nome_mun = 'graccho cardoso'
                then 'gracho cardoso'
                when nome_mun = 'passa vinte'
                then 'passa-vinte'
                when nome_mun = 'parati'
                then 'paraty'
                when nome_mun = 'balneario de picarras'
                then 'balneario picarras'
                when nome_mun = 'mogi-guacu'
                then 'mogi guacu'
                when nome_mun = 'sao luiz do paraitinga'
                then 'sao luis do paraitinga'
                when nome_mun = 'santana do livramento'
                then "sant'ana do livramento"
                when nome_mun = 'belem de sao francisco'
                then 'belem do sao francisco'
                when nome_mun = 'barao do monte alto'
                then 'barao de monte alto'
                when nome_mun = 'sao tome das letras'
                then 'sao thome das letras'
                when nome_mun = 'brasopolis'
                then 'brazopolis'
                when nome_mun = 'florinea'
                then 'florinia'
                when nome_mun = 'sao valerio da natividade'
                then 'sao valerio'
                when nome_mun = 'santa cruz do monte castelo'
                then 'santa cruz de monte castelo'
                when nome_mun = 'poxoreu'
                then 'poxoreo'
                when nome_mun = 'pindare mirim'
                then 'pindare-mirim'
                when nome_mun = 'entre ijuis'
                then 'entre-ijuis'
                when nome_mun = 'assu'
                then 'acu'
                when nome_mun = 'amparo da serra'
                then 'amparo do serra'
                when nome_mun = 'dona euzebia'
                then 'dona eusebia'
                when nome_mun = 'eldorado dos carajas'
                then 'eldorado do carajas'
                when nome_mun = 'couto de magalhaes'
                then 'couto magalhaes'
                when nome_mun = 'sao domingos de pombal'
                then 'sao domingos'
                when nome_mun = 'picarras'
                then 'balneario picarras'
                when nome_mun = "pingo d'agua"
                then "pingo-d'agua"
                when nome_mun = 'suzanopolis'
                then 'suzanapolis'
                when nome_mun = 'suzanopolis'
                then 'suzanapolis'
                when nome_mun = 'povoado pouso alegre'
                then 'pouso alegre'
                when nome_mun = 'alta floresta d oeste'
                then "alta floresta d'oeste"
                when nome_mun = 'santa luzia d oeste'
                then "santa luzia d'oeste"
                when nome_mun = "machadinho d oeste"
                then "machadinho d'oeste"
                when nome_mun = "gloria d oeste"
                then "gloria d'oeste"
                when nome_mun = "alvorada d oeste"
                then "alvorada d'oeste"
                when nome_mun = "bom jesus" and sigla_uf = 'GO'
                then "bom jesus de goias"
                when nome_mun = "presidente castelo branco" and sigla_uf = 'SC'
                then 'presidente castello branco'
                when nome_mun = "santarem" and sigla_uf = 'PB'
                then 'joca claudino'
                else nome_mun
            end as nome_mun,
            *
        from lower_munis
        left join
            (
                select
                    lower(
                        regexp_replace(normalize(nome, nfd), r"\pM", '')
                    ) nome_municipio,
                    id_municipio,
                    sigla_uf as sigla_uf1
                from basedosdados.br_bd_diretorios_brasil.municipio
            ) as mun
            on lower_munis.nome_mun = mun.nome_municipio
            and lower_munis.sigla_uf = mun.sigla_uf1
    ),
    final as (
        select
            safe_cast(data as date) data_referencia,
            safe_cast(
                format_date(
                    '%Y-%m-%d', safe.parse_date('%Y%m%d', data_inscricao)
                ) as date
            ) as data_inscricao,
            safe_cast(id_imovel_receita_federal as string) id_imovel_receita_federal,
            safe_cast(id_imovel_incra as string) id_imovel_incra,
            safe_cast(nome as string) nome,
            safe_cast(area as float64) area,
            safe_cast(cd_rever as string) status_sncr,
            safe_cast(status_rever as string) tipo_itr,
            safe_cast(situacao as string) situacao_imovel,
            safe_cast(endereco as string) endereco,
            safe_cast(cep as string) cep,
            safe_cast(zona_redefinir as string) distrito,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(sigla_uf as string) sigla_uf,
        -- - esta coluna não é identifica no dicionário nem nomeada nos arquivos
        -- - SAFE_CAST(LOWER(status_rever) as STRING) coluna_nao_identificada,
        from fixed_names as t
    )
select *
from final
{% if is_incremental() %}
    where data_referencia > (select max(data_referencia) from {{ this }})
{% endif %}
