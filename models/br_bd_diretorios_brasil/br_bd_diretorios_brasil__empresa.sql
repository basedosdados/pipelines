{{
    config(
        alias="empresa",
        schema="br_bd_diretorios_brasil",
        materialized="table",
        cluster_by=["id_municipio", "sigla_uf"],
        labels={"tema": "economia"},
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (TRUE)',
        ],
    )
}}
--
with
    max_bdpro_date as (
        select max(data) as max_date from `basedosdados.br_me_cnpj.estabelecimentos`
    ),

    estabelecimento as (
        select distinct
            a.cnpj,
            cnpj_basico,
            cnpj_ordem,
            cnpj_dv,
            nome_fantasia,
            cnae_fiscal_principal,
            cnae_fiscal_secundaria,
            case when sigla_uf = 'BR' then 'RJ' else sigla_uf end sigla_uf,
            id_pais as id_pais_me,
            case
                when a.id_pais = '8'
                then 'Brasil'
                when a.id_pais = '9'
                then 'Brasil'
                when
                    id_pais is null
                    and sigla_uf in (
                        'RO',
                        'AC',
                        'AM',
                        'RR',
                        'PA',
                        'AP',
                        'TO',
                        'MA',
                        'PI',
                        'CE',
                        'RN',
                        'PB',
                        'PE',
                        'AL',
                        'SE',
                        'BA',
                        'MG',
                        'ES',
                        'RJ',
                        'SP',
                        'PR',
                        'SC',
                        'RS',
                        'MS',
                        'MT',
                        'GO',
                        'DF',
                        'BR'
                    )
                then 'Brasil'
                else no_pais
            end nome_pais_me,
            case
                when a.id_pais = '8'
                then 'BRA'
                when a.id_pais = '9'
                then 'BRA'
                when
                    id_pais is null
                    and sigla_uf in (
                        'RO',
                        'AC',
                        'AM',
                        'RR',
                        'PA',
                        'AP',
                        'TO',
                        'MA',
                        'PI',
                        'CE',
                        'RN',
                        'PB',
                        'PE',
                        'AL',
                        'SE',
                        'BA',
                        'MG',
                        'ES',
                        'RJ',
                        'SP',
                        'PR',
                        'SC',
                        'RS',
                        'MS',
                        'MT',
                        'GO',
                        'DF',
                        'BR'
                    )
                then 'BRA'
                when
                    a.id_pais is null
                    and sigla_uf not in (
                        'RO',
                        'AC',
                        'AM',
                        'RR',
                        'PA',
                        'AP',
                        'TO',
                        'MA',
                        'PI',
                        'CE',
                        'RN',
                        'PB',
                        'PE',
                        'AL',
                        'SE',
                        'BA',
                        'MG',
                        'ES',
                        'RJ',
                        'SP',
                        'PR',
                        'SC',
                        'RS',
                        'MS',
                        'MT',
                        'GO',
                        'DF',
                        'BR'
                    )
                then code_iso3
                else co_pais_isoa3
            end id_code_iso3,
            b.valor as matriz_filial,
            t.valor as situacao_cadastral,
            situacao_especial,
            cep,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            a.bairro,
            id_municipio,
            id_municipio_rf,
            concat(ddd_1, " ", telefone_1) as telefone_1,
            concat(ddd_2, " ", telefone_2) as telefone_2,
            concat(ddd_fax, " ", fax) as fax,
            email,
        from `basedosdados.br_me_cnpj.estabelecimentos` a
        inner join
            `basedosdados.br_me_cnpj.dicionario` b
            on a.identificador_matriz_filial = b.chave
        inner join
            `basedosdados.br_me_cnpj.dicionario` t
            on a.identificador_matriz_filial = t.chave
        left join
            `basedosdados-dev.br_bd_diretorios_brasil_staging.bairro_code_iso3` g
            on a.bairro = g.bairro
        left join
            `basedosdados-dev.br_bd_diretorios_mundo_staging.pais_code` f
            on a.id_pais = f.co_pais
        where
            a.data = (select max_date from max_bdpro_date)
            and b.nome_coluna = 'identificador_matriz_filial'
            and t.nome_coluna = 'situacao_cadastral'
    ),
    empresa as (
        select distinct
            a.cnpj_basico,
            razao_social,
            natureza_juridica,
            ente_federativo,
            capital_social,
            b.valor as porte,
            a.data
        from `basedosdados.br_me_cnpj.empresas` a
        inner join `basedosdados.br_me_cnpj.dicionario` b on a.porte = b.chave
        where b.nome_coluna = 'porte' and a.data = (select max_date from max_bdpro_date)
    ),
    simples as (
        select distinct cnpj_basico, opcao_simples, opcao_mei
        from `basedosdados.br_me_cnpj.simples`
    )

select
    cnpj,
    a.cnpj_basico,
    a.cnpj_ordem,
    cnpj_dv,
    razao_social,
    nome_fantasia,
    natureza_juridica,
    ente_federativo,
    cnae_fiscal_principal,
    cnae_fiscal_secundaria,
    capital_social,
    porte,
    matriz_filial,
    situacao_cadastral,
    situacao_especial,
    opcao_simples,
    opcao_mei,
    cep,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    id_municipio,
    a.sigla_uf,
    id_code_iso3,
    id_pais_me,
    nome_pais_me,
    telefone_1,
    telefone_2,
    fax,
    email,
from estabelecimento a
left join empresa b on a.cnpj_basico = b.cnpj_basico
left join simples c on a.cnpj_basico = c.cnpj_basico
