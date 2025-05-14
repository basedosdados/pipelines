{{
    config(
        alias="beef",
        schema="br_trase_supply_chain",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2017, "interval": 1},
        },
    )
}}


-- padronizar iso3
with
    inserir_id_iso3 as (
        -- padronizar colunas que precisam ser tratadas
        select
            *,
            substr(trase_geocode, 4, 11) as municipality_id,
            lower(
                translate(
                    `COUNTRY OF DESTINATION`,
                    'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                    'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                )
            ) as name_country_first_import,
            lower(
                translate(
                    `LOGISTICS HUB`,
                    'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                    'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                )
            ) name_logistics_hub
        from {{ set_datalake_project("br_trase_supply_chain_staging.beef") }}

    ),
    iso3 as (
        select *
        from inserir_id_iso3
        left join
            (
                select
                    lower(
                        translate(
                            nome_ingles,
                            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                            'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                        )
                    ) as nome_ingles,
                    sigla_pais_iso3 as iso3_country_id
                from `basedosdados.br_bd_diretorios_mundo.pais`
            ) as diretorio_pais
            on inserir_id_iso3.name_country_first_import = diretorio_pais.nome_ingles
    ),
    iso3_2 as (

        select
            *,
            case
                -- tem valores unknown country e unknown country european union
                -- netherlands antilles -> dissolvida em 2010 para curacao e saint
                -- martin https://2009-2017.state.gov/r/pa/ei/bgn/22528.htm
                -- pacific islands (usa) -> não tem no diretório de países
                when
                    name_country_first_import = 'china (mainland)'
                    and iso3_country_id is null
                then 'CHN'
                when
                    name_country_first_import = 'netherlands'
                    and iso3_country_id is null
                then 'NLD'
                when
                    name_country_first_import = 'united kingdom'
                    and iso3_country_id is null
                then 'GBR'
                when name_country_first_import = 'vietnam' and iso3_country_id is null
                then 'VNM'
                when
                    name_country_first_import = 'united states'
                    and iso3_country_id is null
                then 'USA'
                when
                    name_country_first_import = 'south korea'
                    and iso3_country_id is null
                then 'KOR'
                when name_country_first_import = 'taiwan' and iso3_country_id is null
                then 'TWN'
                when name_country_first_import = 'iran' and iso3_country_id is null
                then 'IRN'
                when name_country_first_import = 'venezuela' and iso3_country_id is null
                then 'VEN'
                when
                    name_country_first_import = 'russian federation'
                    and iso3_country_id is null
                then 'RUS'
                when
                    name_country_first_import = 'united arab emirates'
                    and iso3_country_id is null
                then 'ARE'
                when name_country_first_import = 'bolivia' and iso3_country_id is null
                then 'BOL'
                when
                    name_country_first_import = 'dominican republic'
                    and iso3_country_id is null
                then 'DOM'
                when
                    name_country_first_import = 'philippines'
                    and iso3_country_id is null
                then 'PHL'
                when
                    name_country_first_import = 'china (hong kong)'
                    and iso3_country_id is null
                then 'HKG'
                when
                    name_country_first_import = 'north korea'
                    and iso3_country_id is null
                then 'PRK'
                when
                    name_country_first_import = 'cayman islands'
                    and iso3_country_id is null
                then 'CYM'
                when
                    name_country_first_import = 'turks and caicos islands'
                    and iso3_country_id is null
                then 'TCA'
                when
                    name_country_first_import = 'cape verde' and iso3_country_id is null
                then 'CPV'
                when name_country_first_import = 'bahamas' and iso3_country_id is null
                then 'BHS'
                when name_country_first_import = 'gambia' and iso3_country_id is null
                then 'GMB'
                when name_country_first_import = 'congo' and iso3_country_id is null
                then 'COG'
                when name_country_first_import = 'sudan' and iso3_country_id is null
                then 'SDN'
                when name_country_first_import = 'tanzania' and iso3_country_id is null
                then 'TZA'
                when
                    name_country_first_import = 'virgin islands (uk)'
                    and iso3_country_id is null
                then 'VGB'
                when
                    name_country_first_import = 'netherlands antilles'
                    and iso3_country_id is null
                then 'NLD'
                when
                    name_country_first_import = 'pacific islands (usa)'
                    and iso3_country_id is null
                then 'HKG'
                when name_country_first_import = 'syria' and iso3_country_id is null
                then 'SYR'
                when
                    name_country_first_import = 'congo democratic republic of the'
                    and iso3_country_id is null
                then 'COD'
                when
                    name_country_first_import = 'st. vincent and the grenadines'
                    and iso3_country_id is null
                then 'VCT'
                when
                    name_country_first_import = 'united states virgin islands'
                    and iso3_country_id is null
                then 'VIR'
                when
                    name_country_first_import = 'dominica island'
                    and iso3_country_id is null
                then 'DMA'
                when name_country_first_import = 'macedonia' and iso3_country_id is null
                then 'MKD'
                when
                    name_country_first_import = 'marshall islands'
                    and iso3_country_id is null
                then 'MHL'
                when
                    name_country_first_import = 'st. kitts and nevis'
                    and iso3_country_id is null
                then 'KNA'
                when name_country_first_import = 'moldova' and iso3_country_id is null
                then 'MDA'
                -- canaries islands fazem parte da Espanha
                when
                    name_country_first_import = 'canaries islands'
                    and iso3_country_id is null
                then 'ESP'
                when
                    name_country_first_import = 'falkland islands (malvinas)'
                    and iso3_country_id is null
                then 'FLK'
                when name_country_first_import = 'brunei' and iso3_country_id is null
                then 'BRN'
                when
                    name_country_first_import = 'micronesia' and iso3_country_id is null
                then 'FSM'
                when name_country_first_import = 'niger' and iso3_country_id is null
                then 'NER'
                when
                    name_country_first_import = 'french guyana'
                    and iso3_country_id is null
                then 'GUF'
                when name_country_first_import = 'st martin' and iso3_country_id is null
                then 'MAF'
                when name_country_first_import = 'comoros' and iso3_country_id is null
                then 'COM'
                -- occupied palestinian territory será atribuido a iso3 da  palestina
                when
                    name_country_first_import = 'occupied palestinian territory'
                    and iso3_country_id is null
                then 'PSE'
                else iso3_country_id
            end as iso3_country_id_,
            case
                when `COUNTRY OF PRODUCTION` = 'BRAZIL'
                then 'BRA'
                else `COUNTRY OF PRODUCTION`
            end as country_production_iso3_id,

            -- alguns valores da variável TRASE GEOCODE
            -- não são ids_municipios, o código seguinte corrige isso
            case
                when regexp_contains(municipality_id, r'\D')
                then null
                else municipality_id
            end as municipality_id_production,
            case
                when state = 'ACRE'
                then 'AC'
                when state = 'ALAGOAS'
                then 'AL'
                when state = 'AMAPA'
                then 'AP'
                when state = 'AMAZONAS'
                then 'AM'
                when state = 'BAHIA'
                then 'BA'
                when state = 'CEARA'
                then 'CE'
                when state = 'DISTRITO FEDERAL'
                then 'DF'
                when state = 'ESPIRITO SANTO'
                then 'ES'
                when state = 'GOIAS'
                then 'GO'
                when state = 'MARANHAO'
                then 'MA'
                when state = 'MATO GROSSO'
                then 'MT'
                when state = 'MATO GROSSO DO SUL'
                then 'MS'
                when state = 'MINAS GERAIS'
                then 'MG'
                when state = 'PARA'
                then 'PA'
                when state = 'PARAIBA'
                then 'PB'
                when state = 'PARANA'
                then 'PR'
                when state = 'PERNAMBUCO'
                then 'PE'
                when state = 'PIAUI'
                then 'PI'
                when state = 'RIO DE JANEIRO'
                then 'RJ'
                when state = 'RIO GRANDE DO NORTE'
                then 'RN'
                when state = 'RIO GRANDE DO SUL'
                then 'RS'
                when state = 'RONDONIA'
                then 'RO'
                when state = 'RORAIMA'
                then 'RR'
                when state = 'SANTA CATARINA'
                then 'SC'
                when state = 'SAO PAULO'
                then 'SP'
                when state = 'SERGIPE'
                then 'SE'
                when state = 'TOCANTINS'
                then 'TO'
                else ' '
            end as state_production,
        from iso3
    ),
    -- adicionar id_municipio do logistics hub
    add_logistics as (
        select *
        from iso3_2
        left join
            (
                select
                    lower(
                        translate(
                            nome,
                            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                            'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                        )
                    ) as nome,
                    id_municipio as municipality_id_logistics_hub
                from `basedosdados.br_bd_diretorios_brasil.municipio`

            ) as diretorio
            on iso3_2.name_logistics_hub = diretorio.nome
            and diretorio.nome not in (
                'rio branco',
                'belem',
                'redencao',
                'nova olinda',
                'alvorada',
                'varzea grande',
                'hidrolandia',
                'campo grande',
                'pedra preta',
                'santa maria',
                'sertaozinho',
                'sao gabriel',
                'agua boa',
                'ipora',
                'colorado'
            )
    )

select
    safe_cast(year as int64) year,
    safe_cast(biome as string) biome,
    safe_cast(country_production_iso3_id as string) country_production_iso3_id,
    safe_cast(state_production as string) state_production,
    safe_cast(lower(municipality) as string) municipality_name_production,
    safe_cast(municipality_id_production as string) municipality_id_production,
    safe_cast(name_logistics_hub as string) municipality_name_logistics_hub,
    safe_cast(municipality_id_logistics_hub as string) municipality_id_logistics_hub,
    safe_cast(replace(exporter, 'UNKNOWN', '') as string) exporter_name,
    safe_cast(replace(`EXPORTER GROUP`, 'UNKNOWN', '') as string) exporter_group,
    safe_cast(replace(importer, 'UNKNOWN', '') as string) importer_name,
    safe_cast(replace(`IMPORTER GROUP`, 'UNKNOWN', '') as string) importer_group,
    safe_cast(iso3_country_id_ as string) country_first_import_iso3_id,
    safe_cast(`COUNTRY OF DESTINATION` as string) country_first_import_name,
    safe_cast(`ECONOMIC BLOC` as string) economic_bloc,
    safe_cast(fob_usd as float64) fob_usd,
    safe_cast(beef_equivalent_tonnes as float64) beef_equivalent_tonnes,
    safe_cast(product_descr as string) product_description,
    safe_cast(
        replace(zero_deforestation_brazil_beef, 'NONE', '') as string
    ) zero_deforestation_commitments,
    safe_cast(
        `CATTLE_DEFORESTATION_5_YEAR_TOTAL_EXPOSURE` as string
    ) cattle_deforestation_exposure,
    safe_cast(
        co2_emissions_cattle_deforestation_5_year_total_exposure as float64
    ) co2_emissions_deforestation_5,
    safe_cast(type as string) type
from add_logistics as t
