{{
    config(
        alias="dicionario",
        schema="world_oecd_socx",
        materialized="table",
    )
}}

-- Every coded value that appears in this dataset, with the label the OECD
-- publishes for it in the SDMX codelists. The facts carry codes only -- the
-- labels are structure metadata, not data -- so this is a literal map rather
-- than a select over the fact models. custom_dictionary_coverage in schema.yml
-- enforces that it covers every value actually present.
select id_tabela, nome_coluna, chave, cobertura_temporal, valor
from
    unnest(
        [
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'frequency' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Annual' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'measure' as nome_coluna,
                'SOCX' as chave,
                '' as cobertura_temporal,
                'Social expenditure' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_B1GQ' as chave,
                '' as cobertura_temporal,
                'Percentage of GDP' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_OTE_S13' as chave,
                '' as cobertura_temporal,
                'Percentage of general government expenditure' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP_PS' as chave,
                '' as cobertura_temporal,
                'US dollars per person, PPP converted' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES10' as chave,
                '' as cobertura_temporal,
                'Public' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES10_20' as chave,
                '' as cobertura_temporal,
                'Public and mandatory private' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES20' as chave,
                '' as cobertura_temporal,
                'Mandatory private' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES20_30' as chave,
                '' as cobertura_temporal,
                'Mandatory private and voluntary private' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES30' as chave,
                '' as cobertura_temporal,
                'Voluntary private' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES40' as chave,
                '' as cobertura_temporal,
                'Net public' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'expenditure_source' as nome_coluna,
                'ES50' as chave,
                '' as cobertura_temporal,
                'Net total' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'spending_type' as nome_coluna,
                'C' as chave,
                '' as cobertura_temporal,
                'In cash spending' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'spending_type' as nome_coluna,
                'K' as chave,
                '' as cobertura_temporal,
                'In kind spending' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'spending_type' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP01' as chave,
                '' as cobertura_temporal,
                'Old age and survivors' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP11' as chave,
                '' as cobertura_temporal,
                'Old age' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP111' as chave,
                '' as cobertura_temporal,
                'Pension' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP112' as chave,
                '' as cobertura_temporal,
                'Early retirement pension' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP113' as chave,
                '' as cobertura_temporal,
                'Other cash benefits' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP121' as chave,
                '' as cobertura_temporal,
                'Residential care / Home' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP122' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP21' as chave,
                '' as cobertura_temporal,
                'Survivors' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP211' as chave,
                '' as cobertura_temporal,
                'Pension' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP212' as chave,
                '' as cobertura_temporal,
                'Other cash benefits' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP221' as chave,
                '' as cobertura_temporal,
                'Funeral expenses' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP222' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP31' as chave,
                '' as cobertura_temporal,
                'Incapacity related' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP311' as chave,
                '' as cobertura_temporal,
                'Disability pensions' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP312' as chave,
                '' as cobertura_temporal,
                'Pensions (occupational injury and disease)' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP313' as chave,
                '' as cobertura_temporal,
                'Paid sick leave (occupational injury and disease)' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP314' as chave,
                '' as cobertura_temporal,
                'Paid sick leave (other sickness daily allowances)' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP315' as chave,
                '' as cobertura_temporal,
                'Other cash benefits' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP321' as chave,
                '' as cobertura_temporal,
                'Residential care / Home' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP322' as chave,
                '' as cobertura_temporal,
                'Rehabilitation services' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP323' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP41' as chave,
                '' as cobertura_temporal,
                'Health' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP51' as chave,
                '' as cobertura_temporal,
                'Family' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP511' as chave,
                '' as cobertura_temporal,
                'Family allowances' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP512' as chave,
                '' as cobertura_temporal,
                'Maternity and parental leave' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP513' as chave,
                '' as cobertura_temporal,
                'Other cash benefits' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP521' as chave,
                '' as cobertura_temporal,
                'Early childhood education and care (ECEC)' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP522' as chave,
                '' as cobertura_temporal,
                'Home help / Accomodation' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP523' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP60' as chave,
                '' as cobertura_temporal,
                'Active labour market programmes' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP601' as chave,
                '' as cobertura_temporal,
                'PES and Administration' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP602' as chave,
                '' as cobertura_temporal,
                'Training' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP603' as chave,
                '' as cobertura_temporal,
                'Job Rotation and Job Sharing' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP604' as chave,
                '' as cobertura_temporal,
                'Employment Incentives' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP605' as chave,
                '' as cobertura_temporal,
                'Supported Employment and Rehabilitation' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP606' as chave,
                '' as cobertura_temporal,
                'Direct Job Creation' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP607' as chave,
                '' as cobertura_temporal,
                'Start' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP71' as chave,
                '' as cobertura_temporal,
                'Unemployment' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP711' as chave,
                '' as cobertura_temporal,
                'Unemployment compensation / severance pay' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP712' as chave,
                '' as cobertura_temporal,
                'Early retirement for labour market reasons' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP82' as chave,
                '' as cobertura_temporal,
                'Housing' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP821' as chave,
                '' as cobertura_temporal,
                'Housing assistance' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP822' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP91' as chave,
                '' as cobertura_temporal,
                'Other social policy areas' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP911' as chave,
                '' as cobertura_temporal,
                'Income maintenance' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP912' as chave,
                '' as cobertura_temporal,
                'Other cash benefits' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP921' as chave,
                '' as cobertura_temporal,
                'Social assistance' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                'TP922' as chave,
                '' as cobertura_temporal,
                'Other benefits in kind' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'programme_type' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'price_base' as nome_coluna,
                'Q' as chave,
                '' as cobertura_temporal,
                'Constant prices' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'AUD' as chave,
                '' as cobertura_temporal,
                'Australian dollar' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'CAD' as chave,
                '' as cobertura_temporal,
                'Canadian dollar' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'CHF' as chave,
                '' as cobertura_temporal,
                'Swiss franc' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'CLP' as chave,
                '' as cobertura_temporal,
                'Chilean peso' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'COP' as chave,
                '' as cobertura_temporal,
                'Colombian peso' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'CRC' as chave,
                '' as cobertura_temporal,
                'Costa Rican colon' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'CZK' as chave,
                '' as cobertura_temporal,
                'Czech koruna' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'DKK' as chave,
                '' as cobertura_temporal,
                'Danish krone' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'EUR' as chave,
                '' as cobertura_temporal,
                'Euro' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'GBP' as chave,
                '' as cobertura_temporal,
                'Pound sterling' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'HUF' as chave,
                '' as cobertura_temporal,
                'Forint' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'ILS' as chave,
                '' as cobertura_temporal,
                'New Israeli sheqel' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'ISK' as chave,
                '' as cobertura_temporal,
                'Iceland krona' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'JPY' as chave,
                '' as cobertura_temporal,
                'Yen' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'KRW' as chave,
                '' as cobertura_temporal,
                'Won' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'MXN' as chave,
                '' as cobertura_temporal,
                'Mexican peso' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'NOK' as chave,
                '' as cobertura_temporal,
                'Norwegian krone' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'NZD' as chave,
                '' as cobertura_temporal,
                'New Zealand dollar' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'PLN' as chave,
                '' as cobertura_temporal,
                'Zloty' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'RON' as chave,
                '' as cobertura_temporal,
                'Romanian leu' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'SEK' as chave,
                '' as cobertura_temporal,
                'Swedish krona' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'TRY' as chave,
                '' as cobertura_temporal,
                'Turkish lira' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                'USD' as chave,
                '' as cobertura_temporal,
                'US dollar' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'currency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'decimals' as nome_coluna,
                '1' as chave,
                '' as cobertura_temporal,
                'One' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'expenditure' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '6' as chave,
                '' as cobertura_temporal,
                'Millions' as valor
            )
        ]
    )
