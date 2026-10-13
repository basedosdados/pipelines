{{
    config(
        alias="dicionario",
        schema="world_oecd_education",
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
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'G20' as chave,
                '' as cobertura_temporal,
                'G20' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'student' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_0' as chave,
                '' as cobertura_temporal,
                'Early childhood education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_01' as chave,
                '' as cobertura_temporal,
                'Early childhood educational development' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_25' as chave,
                '' as cobertura_temporal,
                'Lower secondary vocational education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_341' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education, insufficient for level completion or partial level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_342' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education, sufficient for partial level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_343' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education, sufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_344' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education, sufficient for level completion, with direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_351' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education, insufficient for level completion or partial level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_352' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education, sufficient for partial level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_353' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education, sufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_354' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education, sufficient for level completion, with direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35SW' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education, school and work-based programmes'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_44' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_441' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education, insufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_443' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education, sufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_444' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education, sufficient for level completion, with direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_45' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_451' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education, insufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_453' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education, sufficient for level completion, without direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_454' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education, sufficient for level completion, with direct access to tertiary education'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_45SW' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education, school and work-based programmes'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_54' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary general education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_541' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary general education, insufficient for level completion'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_544' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary general education, sufficient for level completion'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_55' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary vocational education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_551' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary vocational education, insufficient for level completion'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_554' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary vocational education, sufficient for level completion'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_55SW' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary vocational education, school and work-based programmes'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T7' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary, Bachelor\'s, Master\'s or equivalent level'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_6' as chave,
                '' as cobertura_temporal,
                'Bachelor’s or equivalent level' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_641_651_661' as chave,
                '' as cobertura_temporal,
                'Bachelor’s or equivalent level, insufficient for  completion' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_645_655_665' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s or equivalent level, first degree (3-4 years)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_646_656_666' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s or equivalent level, first long degree (more than 4 years)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_647_657_667' as chave,
                '' as cobertura_temporal,
                'Second or further degree following successful completion of a Bachelor\'s or equivalent programme'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_7' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_741_751_761' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level, all programmes, insufficient for level completion'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_746_756_766' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level, long first degree (at least 5 years)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_747_757_767' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level, second or further degree (following a Bachelor\'s or equivalent programme)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_748_758_768' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level, second or further degree (following a Master’s or equivalent programme)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_8' as chave,
                '' as cobertura_temporal,
                'Doctoral or equivalent level' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_level' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'student' as id_tabela,
                'measure' as nome_coluna,
                'ENRL' as chave,
                '' as cobertura_temporal,
                'Students enrolled' as valor
            ),
            struct(
                'student' as id_tabela,
                'measure' as nome_coluna,
                'GRAD' as chave,
                '' as cobertura_temporal,
                'Graduates' as valor
            ),
            struct(
                'student' as id_tabela,
                'measure' as nome_coluna,
                'NENT' as chave,
                '' as cobertura_temporal,
                'New entrants' as valor
            ),
            struct(
                'student' as id_tabela,
                'measure' as nome_coluna,
                'RPTR' as chave,
                '' as cobertura_temporal,
                'Repeaters' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_type' as nome_coluna,
                'FE' as chave,
                '' as cobertura_temporal,
                'Formal education, includes formal initial and formal adult education programmes'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_type' as nome_coluna,
                'FE_ADLT' as chave,
                '' as cobertura_temporal,
                'Formal adult education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_type' as nome_coluna,
                'FE_INIT' as chave,
                '' as cobertura_temporal,
                'Formal initial education' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_type' as nome_coluna,
                'ISCED11_OUT' as chave,
                '' as cobertura_temporal,
                'Registered ECEC services outside ISCED' as valor
            ),
            struct(
                'student' as id_tabela,
                'intensity' as nome_coluna,
                'FT' as chave,
                '' as cobertura_temporal,
                'Full-time' as valor
            ),
            struct(
                'student' as id_tabela,
                'intensity' as nome_coluna,
                'PT' as chave,
                '' as cobertura_temporal,
                'Part-time' as valor
            ),
            struct(
                'student' as id_tabela,
                'intensity' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F00' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F000' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications not further defined (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0000' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications not further defined (detailed field)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F001' as chave,
                '' as cobertura_temporal,
                'Basic programmes and qualifications (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0011' as chave,
                '' as cobertura_temporal,
                'Basic programmes and qualifications (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F002' as chave,
                '' as cobertura_temporal,
                'Literacy and numeracy (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0021' as chave,
                '' as cobertura_temporal,
                'Literacy and numeracy (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F003' as chave,
                '' as cobertura_temporal,
                'Personal skills and development (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0031' as chave,
                '' as cobertura_temporal,
                'Personal skills and development (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F009' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0099' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F01' as chave,
                '' as cobertura_temporal,
                'Education (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F011' as chave,
                '' as cobertura_temporal,
                'Education (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0110' as chave,
                '' as cobertura_temporal,
                'Education not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0111' as chave,
                '' as cobertura_temporal,
                'Education science' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0112' as chave,
                '' as cobertura_temporal,
                'Training for pre-school teachers' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0113' as chave,
                '' as cobertura_temporal,
                'Teacher training without subject specialisation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0114' as chave,
                '' as cobertura_temporal,
                'Teacher training with subject specialisation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0119' as chave,
                '' as cobertura_temporal,
                'Education not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F018' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving education (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0188' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving education (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F02' as chave,
                '' as cobertura_temporal,
                'Arts and humanities' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F020' as chave,
                '' as cobertura_temporal,
                'Arts and humanities not further defined (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0200' as chave,
                '' as cobertura_temporal,
                'Arts and humanities not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F021' as chave,
                '' as cobertura_temporal,
                'Arts' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0210' as chave,
                '' as cobertura_temporal,
                'Arts not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0211' as chave,
                '' as cobertura_temporal,
                'Audio-visual techniques and media production' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0212' as chave,
                '' as cobertura_temporal,
                'Fashion, interior and industrial design' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0213' as chave,
                '' as cobertura_temporal,
                'Fine arts' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0214' as chave,
                '' as cobertura_temporal,
                'Handicrafts' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0215' as chave,
                '' as cobertura_temporal,
                'Music and performing arts' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0219' as chave,
                '' as cobertura_temporal,
                'Arts not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F022' as chave,
                '' as cobertura_temporal,
                'Humanities (except languages)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0220' as chave,
                '' as cobertura_temporal,
                'Humanities (except languages) not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0221' as chave,
                '' as cobertura_temporal,
                'Religion and  theology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0222' as chave,
                '' as cobertura_temporal,
                'History and archaeology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0223' as chave,
                '' as cobertura_temporal,
                'Philosophy and ethics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0229' as chave,
                '' as cobertura_temporal,
                'Humanities (except languages) not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F023' as chave,
                '' as cobertura_temporal,
                'Languages' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0230' as chave,
                '' as cobertura_temporal,
                'Languages not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0231' as chave,
                '' as cobertura_temporal,
                'Language acquisition' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0232' as chave,
                '' as cobertura_temporal,
                'Literature and linguistics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0239' as chave,
                '' as cobertura_temporal,
                'Languages not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F028' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving arts and humanities (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0288' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving arts and humanities (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F029' as chave,
                '' as cobertura_temporal,
                'Arts and humanities not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0299' as chave,
                '' as cobertura_temporal,
                'Arts and humanities not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F03' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F030' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information not further defined (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0300' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F031' as chave,
                '' as cobertura_temporal,
                'Social and behavioural sciences' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0310' as chave,
                '' as cobertura_temporal,
                'Social and behavioural sciences not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0311' as chave,
                '' as cobertura_temporal,
                'Economics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0312' as chave,
                '' as cobertura_temporal,
                'Political sciences and civics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0313' as chave,
                '' as cobertura_temporal,
                'Psychology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0314' as chave,
                '' as cobertura_temporal,
                'Sociology and cultural studies' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0319' as chave,
                '' as cobertura_temporal,
                'Social and behavioural sciences not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F032' as chave,
                '' as cobertura_temporal,
                'Journalism and information' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0320' as chave,
                '' as cobertura_temporal,
                'Journalism and information not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0321' as chave,
                '' as cobertura_temporal,
                'Journalism and reporting' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0322' as chave,
                '' as cobertura_temporal,
                'Library, information and archival studies' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0329' as chave,
                '' as cobertura_temporal,
                'Journalism and information not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F038' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving social sciences, journalism and information (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0388' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving social sciences, journalism and information (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F039' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0399' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F04' as chave,
                '' as cobertura_temporal,
                'Business, administration and law' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F040' as chave,
                '' as cobertura_temporal,
                'Business, administration and law not further defined (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0400' as chave,
                '' as cobertura_temporal,
                'Business, administration and law not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F041' as chave,
                '' as cobertura_temporal,
                'Business and administration' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0410' as chave,
                '' as cobertura_temporal,
                'Business and administration not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0411' as chave,
                '' as cobertura_temporal,
                'Accounting and taxation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0412' as chave,
                '' as cobertura_temporal,
                'Finance, banking and insurance' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0413' as chave,
                '' as cobertura_temporal,
                'Management and administration' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0414' as chave,
                '' as cobertura_temporal,
                'Marketing and advertising' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0415' as chave,
                '' as cobertura_temporal,
                'Secretarial and office work' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0416' as chave,
                '' as cobertura_temporal,
                'Wholesale and retail sales' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0417' as chave,
                '' as cobertura_temporal,
                'Work skills' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0419' as chave,
                '' as cobertura_temporal,
                'Business and administration not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F042' as chave,
                '' as cobertura_temporal,
                'Law (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0421' as chave,
                '' as cobertura_temporal,
                'Law (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F048' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving business, administration and law (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0488' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving business, administration and law (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F049' as chave,
                '' as cobertura_temporal,
                'Business, administration and law not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0499' as chave,
                '' as cobertura_temporal,
                'Business, administration and law not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F05' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F050' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics not further defined'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0500' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F051' as chave,
                '' as cobertura_temporal,
                'Biological and related sciences' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0510' as chave,
                '' as cobertura_temporal,
                'Biological and related sciences not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0511' as chave,
                '' as cobertura_temporal,
                'Biology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0512' as chave,
                '' as cobertura_temporal,
                'Biochemistry' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0519' as chave,
                '' as cobertura_temporal,
                'Biological and related sciences not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F052' as chave,
                '' as cobertura_temporal,
                'Environment' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0520' as chave,
                '' as cobertura_temporal,
                'Environment not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0521' as chave,
                '' as cobertura_temporal,
                'Environmental sciences' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0522' as chave,
                '' as cobertura_temporal,
                'Natural environments and wildlife' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0529' as chave,
                '' as cobertura_temporal,
                'Environment not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F053' as chave,
                '' as cobertura_temporal,
                'Physical sciences' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0530' as chave,
                '' as cobertura_temporal,
                'Physical sciences not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0531' as chave,
                '' as cobertura_temporal,
                'Chemistry' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0532' as chave,
                '' as cobertura_temporal,
                'Earth sciences' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0533' as chave,
                '' as cobertura_temporal,
                'Physics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0539' as chave,
                '' as cobertura_temporal,
                'Physical sciences not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F054' as chave,
                '' as cobertura_temporal,
                'Mathematics and statistics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0540' as chave,
                '' as cobertura_temporal,
                'Mathematics and statistics not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0541' as chave,
                '' as cobertura_temporal,
                'Mathematics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0542' as chave,
                '' as cobertura_temporal,
                'Statistics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F058' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving natural sciences, mathematics and statistics (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0588' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving natural sciences, mathematics and statistics (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F059' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0599' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F05T07' as chave,
                '' as cobertura_temporal,
                'Science, technology, engineering and mathematics' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F06' as chave,
                '' as cobertura_temporal,
                'Information and Communication Technologies (ICTs) (broad field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F061' as chave,
                '' as cobertura_temporal,
                'Information and Communication Technologies (ICTs) (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0610' as chave,
                '' as cobertura_temporal,
                'Information and Communication Technologies (ICTs) not further defined'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0611' as chave,
                '' as cobertura_temporal,
                'Computer use' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0612' as chave,
                '' as cobertura_temporal,
                'Database and network design and administration' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0613' as chave,
                '' as cobertura_temporal,
                'Software and applications development and analysis' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0619' as chave,
                '' as cobertura_temporal,
                'Information and Communication Technologies (ICTs) not elsewhere classified'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F068' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving Information and Communication Technologies (ICTs) (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0688' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving Information and Communication Technologies (ICTs) (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F07' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F070' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction not further defined (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0700' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F071' as chave,
                '' as cobertura_temporal,
                'Engineering and engineering trades' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0710' as chave,
                '' as cobertura_temporal,
                'Engineering and engineering trades not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0711' as chave,
                '' as cobertura_temporal,
                'Chemical engineering and processes' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0712' as chave,
                '' as cobertura_temporal,
                'Environmental protection technology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0713' as chave,
                '' as cobertura_temporal,
                'Electricity and energy' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0714' as chave,
                '' as cobertura_temporal,
                'Electronics and automation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0715' as chave,
                '' as cobertura_temporal,
                'Mechanics and metal trades' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0716' as chave,
                '' as cobertura_temporal,
                'Motor vehicles, ships and aircraft' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0719' as chave,
                '' as cobertura_temporal,
                'Engineering and engineering trades not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F072' as chave,
                '' as cobertura_temporal,
                'Manufacturing and processing' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0720' as chave,
                '' as cobertura_temporal,
                'Manufacturing and processing not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0721' as chave,
                '' as cobertura_temporal,
                'Food processing' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0722' as chave,
                '' as cobertura_temporal,
                'Materials (glass, paper, plastic and wood)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0723' as chave,
                '' as cobertura_temporal,
                'Textiles (clothes, footwear and leather)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0724' as chave,
                '' as cobertura_temporal,
                'Mining and extraction' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0729' as chave,
                '' as cobertura_temporal,
                'Manufacturing and processing not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F073' as chave,
                '' as cobertura_temporal,
                'Architecture and construction' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0730' as chave,
                '' as cobertura_temporal,
                'Architecture and construction not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0731' as chave,
                '' as cobertura_temporal,
                'Architecture and town planning' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0732' as chave,
                '' as cobertura_temporal,
                'Building and civil engineering' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F078' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving engineering, manufacturing and construction (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0788' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving engineering, manufacturing and construction (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F079' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0799' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F08' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F080' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary not further defined (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0800' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary not further defined (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F081' as chave,
                '' as cobertura_temporal,
                'Agriculture' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0810' as chave,
                '' as cobertura_temporal,
                'Agriculture not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0811' as chave,
                '' as cobertura_temporal,
                'Crop and livestock production' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0812' as chave,
                '' as cobertura_temporal,
                'Horticulture' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0819' as chave,
                '' as cobertura_temporal,
                'Agriculture not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F082' as chave,
                '' as cobertura_temporal,
                'Forestry (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0821' as chave,
                '' as cobertura_temporal,
                'Forestry (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F083' as chave,
                '' as cobertura_temporal,
                'Fisheries (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0831' as chave,
                '' as cobertura_temporal,
                'Fisheries (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F084' as chave,
                '' as cobertura_temporal,
                'Veterinary (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0841' as chave,
                '' as cobertura_temporal,
                'Veterinary (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F088' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving agriculture, forestry, fisheries and veterinary (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0888' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving agriculture, forestry, fisheries and veterinary (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F089' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0899' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F09' as chave,
                '' as cobertura_temporal,
                'Health and welfare (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F090' as chave,
                '' as cobertura_temporal,
                'Health and welfare not further defined (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0900' as chave,
                '' as cobertura_temporal,
                'Health and welfare not further defined (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F091' as chave,
                '' as cobertura_temporal,
                'Health' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0910' as chave,
                '' as cobertura_temporal,
                'Health not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0911' as chave,
                '' as cobertura_temporal,
                'Dental studies' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0912' as chave,
                '' as cobertura_temporal,
                'Medicine' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0913' as chave,
                '' as cobertura_temporal,
                'Nursing and midwifery' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0914' as chave,
                '' as cobertura_temporal,
                'Medical diagnostic and treatment technology' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0915' as chave,
                '' as cobertura_temporal,
                'Therapy and rehabilitation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0916' as chave,
                '' as cobertura_temporal,
                'Pharmacy' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0917' as chave,
                '' as cobertura_temporal,
                'Traditional and complementary medicine and therapy' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0919' as chave,
                '' as cobertura_temporal,
                'Health not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F092' as chave,
                '' as cobertura_temporal,
                'Welfare' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0920' as chave,
                '' as cobertura_temporal,
                'Welfare not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0921' as chave,
                '' as cobertura_temporal,
                'Care of the elderly and of disabled adults' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0922' as chave,
                '' as cobertura_temporal,
                'Child care and youth services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0923' as chave,
                '' as cobertura_temporal,
                'Social work and counselling' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0929' as chave,
                '' as cobertura_temporal,
                'Welfare not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F098' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving health and welfare (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0988' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving health and welfare (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F099' as chave,
                '' as cobertura_temporal,
                'Health and welfare not elsewhere classified (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F0999' as chave,
                '' as cobertura_temporal,
                'Health and welfare not elsewhere classified (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F10' as chave,
                '' as cobertura_temporal,
                'Services (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F100' as chave,
                '' as cobertura_temporal,
                'Services not further defined (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1000' as chave,
                '' as cobertura_temporal,
                'Services not further defined (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F101' as chave,
                '' as cobertura_temporal,
                'Personal services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1010' as chave,
                '' as cobertura_temporal,
                'Personal services not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1011' as chave,
                '' as cobertura_temporal,
                'Domestic services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1012' as chave,
                '' as cobertura_temporal,
                'Hair and beauty services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1013' as chave,
                '' as cobertura_temporal,
                'Hotel, restaurants and catering' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1014' as chave,
                '' as cobertura_temporal,
                'Sports' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1015' as chave,
                '' as cobertura_temporal,
                'Travel, tourism and leisure' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1019' as chave,
                '' as cobertura_temporal,
                'Personal services not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F102' as chave,
                '' as cobertura_temporal,
                'Hygiene and occupational health services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1020' as chave,
                '' as cobertura_temporal,
                'Hygiene and occupational health services not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1021' as chave,
                '' as cobertura_temporal,
                'Community sanitation' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1022' as chave,
                '' as cobertura_temporal,
                'Occupational health and safety' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1029' as chave,
                '' as cobertura_temporal,
                'Hygiene and occupational health services not elsewhere classified'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F103' as chave,
                '' as cobertura_temporal,
                'Security services' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1030' as chave,
                '' as cobertura_temporal,
                'Security services not further defined' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1031' as chave,
                '' as cobertura_temporal,
                'Military and defence' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1032' as chave,
                '' as cobertura_temporal,
                'Protection of individuals and property' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1039' as chave,
                '' as cobertura_temporal,
                'Security services not elsewhere classified' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F104' as chave,
                '' as cobertura_temporal,
                'Transport services (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1041' as chave,
                '' as cobertura_temporal,
                'Transport services (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F108' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving services (narrow field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1088' as chave,
                '' as cobertura_temporal,
                'Inter-disciplinary programmes and qualifications involving services (detailed field level)'
                as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F109' as chave,
                '' as cobertura_temporal,
                'Services not elsewhere classified (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F1099' as chave,
                '' as cobertura_temporal,
                'Services not elsewhere classified (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F99' as chave,
                '' as cobertura_temporal,
                'Field unspecified (broad field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F999' as chave,
                '' as cobertura_temporal,
                'Field unspecified (narrow field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                'F9999' as chave,
                '' as cobertura_temporal,
                'Field unspecified (detailed field level)' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_field' as nome_coluna,
                '_TX_U' as chave,
                '' as cobertura_temporal,
                'Total (excluding unknown)' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G1' as chave,
                '' as cobertura_temporal,
                'Grade 1' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G2' as chave,
                '' as cobertura_temporal,
                'Grade 2' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G3' as chave,
                '' as cobertura_temporal,
                'Grade 3' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G4' as chave,
                '' as cobertura_temporal,
                'Grade 4' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G5' as chave,
                '' as cobertura_temporal,
                'Grade 5' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G6' as chave,
                '' as cobertura_temporal,
                'Grade 6' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                'G7' as chave,
                '' as cobertura_temporal,
                'Grade 7' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'grade' as nome_coluna,
                '_X' as chave,
                '' as cobertura_temporal,
                'Not allocated/unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'frequency' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Annual' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'A2' as chave,
                '' as cobertura_temporal,
                'Northern America' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'A9' as chave,
                '' as cobertura_temporal,
                'Latin America and the Caribbean' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'E' as chave,
                '' as cobertura_temporal,
                'Europe' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Africa' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Oceania' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'S' as chave,
                '' as cobertura_temporal,
                'Asia' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'World' as valor
            ),
            struct(
                'student' as id_tabela,
                'origin_area' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'A2' as chave,
                '' as cobertura_temporal,
                'Northern America' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'A2_X' as chave,
                '' as cobertura_temporal,
                'Northern America unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'A9' as chave,
                '' as cobertura_temporal,
                'Latin America and the Caribbean' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'A9_X' as chave,
                '' as cobertura_temporal,
                'Latin America and the Caribbean unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ABW' as chave,
                '' as cobertura_temporal,
                'Aruba' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AFG' as chave,
                '' as cobertura_temporal,
                'Afghanistan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AGO' as chave,
                '' as cobertura_temporal,
                'Angola' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AIA' as chave,
                '' as cobertura_temporal,
                'Anguilla' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ALB' as chave,
                '' as cobertura_temporal,
                'Albania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AND' as chave,
                '' as cobertura_temporal,
                'Andorra' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ARE' as chave,
                '' as cobertura_temporal,
                'United Arab Emirates' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ARM' as chave,
                '' as cobertura_temporal,
                'Armenia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ATG' as chave,
                '' as cobertura_temporal,
                'Antigua and Barbuda' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'AZE' as chave,
                '' as cobertura_temporal,
                'Azerbaijan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BDI' as chave,
                '' as cobertura_temporal,
                'Burundi' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BEN' as chave,
                '' as cobertura_temporal,
                'Benin' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BFA' as chave,
                '' as cobertura_temporal,
                'Burkina Faso' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BGD' as chave,
                '' as cobertura_temporal,
                'Bangladesh' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BHR' as chave,
                '' as cobertura_temporal,
                'Bahrain' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BHS' as chave,
                '' as cobertura_temporal,
                'Bahamas' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BIH' as chave,
                '' as cobertura_temporal,
                'Bosnia and Herzegovina' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BLR' as chave,
                '' as cobertura_temporal,
                'Belarus' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BLZ' as chave,
                '' as cobertura_temporal,
                'Belize' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BMU' as chave,
                '' as cobertura_temporal,
                'Bermuda' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BOL' as chave,
                '' as cobertura_temporal,
                'Bolivia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BRB' as chave,
                '' as cobertura_temporal,
                'Barbados' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BRN' as chave,
                '' as cobertura_temporal,
                'Brunei Darussalam' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BTN' as chave,
                '' as cobertura_temporal,
                'Bhutan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'BWA' as chave,
                '' as cobertura_temporal,
                'Botswana' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CAF' as chave,
                '' as cobertura_temporal,
                'Central African Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CHN' as chave,
                '' as cobertura_temporal,
                'China (People’s Republic of)' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CIV' as chave,
                '' as cobertura_temporal,
                'Côte d’Ivoire' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CMR' as chave,
                '' as cobertura_temporal,
                'Cameroon' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'COD' as chave,
                '' as cobertura_temporal,
                'Democratic Republic of the Congo' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'COG' as chave,
                '' as cobertura_temporal,
                'Congo' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'COK' as chave,
                '' as cobertura_temporal,
                'Cook Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'COM' as chave,
                '' as cobertura_temporal,
                'Comoros' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CPV' as chave,
                '' as cobertura_temporal,
                'Cabo Verde' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CUB' as chave,
                '' as cobertura_temporal,
                'Cuba' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CUW' as chave,
                '' as cobertura_temporal,
                'Curaçao' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CYM' as chave,
                '' as cobertura_temporal,
                'Cayman Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CYP' as chave,
                '' as cobertura_temporal,
                'Cyprus' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DJI' as chave,
                '' as cobertura_temporal,
                'Djibouti' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DMA' as chave,
                '' as cobertura_temporal,
                'Dominica' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DOM' as chave,
                '' as cobertura_temporal,
                'Dominican Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'DZA' as chave,
                '' as cobertura_temporal,
                'Algeria' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'E' as chave,
                '' as cobertura_temporal,
                'Europe' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ECU' as chave,
                '' as cobertura_temporal,
                'Ecuador' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'EGY' as chave,
                '' as cobertura_temporal,
                'Egypt' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ERI' as chave,
                '' as cobertura_temporal,
                'Eritrea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ETH' as chave,
                '' as cobertura_temporal,
                'Ethiopia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'E_X' as chave,
                '' as cobertura_temporal,
                'Europe unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Africa' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'FJI' as chave,
                '' as cobertura_temporal,
                'Fiji' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'FSM' as chave,
                '' as cobertura_temporal,
                'Micronesia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'F_X' as chave,
                '' as cobertura_temporal,
                'Africa unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GAB' as chave,
                '' as cobertura_temporal,
                'Gabon' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GEO' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GHA' as chave,
                '' as cobertura_temporal,
                'Ghana' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GIB' as chave,
                '' as cobertura_temporal,
                'Gibraltar' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GIN' as chave,
                '' as cobertura_temporal,
                'Guinea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GMB' as chave,
                '' as cobertura_temporal,
                'Gambia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GNB' as chave,
                '' as cobertura_temporal,
                'Guinea-Bissau' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GNQ' as chave,
                '' as cobertura_temporal,
                'Equatorial Guinea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GRD' as chave,
                '' as cobertura_temporal,
                'Grenada' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GTM' as chave,
                '' as cobertura_temporal,
                'Guatemala' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'GUY' as chave,
                '' as cobertura_temporal,
                'Guyana' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'HKG' as chave,
                '' as cobertura_temporal,
                'Hong Kong (China)' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'HND' as chave,
                '' as cobertura_temporal,
                'Honduras' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'HTI' as chave,
                '' as cobertura_temporal,
                'Haiti' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'IDN' as chave,
                '' as cobertura_temporal,
                'Indonesia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'IND' as chave,
                '' as cobertura_temporal,
                'India' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'IRN' as chave,
                '' as cobertura_temporal,
                'Iran' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'IRQ' as chave,
                '' as cobertura_temporal,
                'Iraq' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'JAM' as chave,
                '' as cobertura_temporal,
                'Jamaica' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'JOR' as chave,
                '' as cobertura_temporal,
                'Jordan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KAZ' as chave,
                '' as cobertura_temporal,
                'Kazakhstan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KEN' as chave,
                '' as cobertura_temporal,
                'Kenya' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KGZ' as chave,
                '' as cobertura_temporal,
                'Kyrgyzstan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KHM' as chave,
                '' as cobertura_temporal,
                'Cambodia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KIR' as chave,
                '' as cobertura_temporal,
                'Kiribati' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KNA' as chave,
                '' as cobertura_temporal,
                'Saint Kitts and Nevis' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'KWT' as chave,
                '' as cobertura_temporal,
                'Kuwait' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LAO' as chave,
                '' as cobertura_temporal,
                'Lao People’s Democratic Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LBN' as chave,
                '' as cobertura_temporal,
                'Lebanon' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LBR' as chave,
                '' as cobertura_temporal,
                'Liberia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LBY' as chave,
                '' as cobertura_temporal,
                'Libya' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LCA' as chave,
                '' as cobertura_temporal,
                'Saint Lucia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LIE' as chave,
                '' as cobertura_temporal,
                'Liechtenstein' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LKA' as chave,
                '' as cobertura_temporal,
                'Sri Lanka' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LSO' as chave,
                '' as cobertura_temporal,
                'Lesotho' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MAC' as chave,
                '' as cobertura_temporal,
                'Macau (China)' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MAR' as chave,
                '' as cobertura_temporal,
                'Morocco' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MCO' as chave,
                '' as cobertura_temporal,
                'Monaco' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MDA' as chave,
                '' as cobertura_temporal,
                'Moldova' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MDG' as chave,
                '' as cobertura_temporal,
                'Madagascar' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MDV' as chave,
                '' as cobertura_temporal,
                'Maldives' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MHL' as chave,
                '' as cobertura_temporal,
                'Marshall Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MKD' as chave,
                '' as cobertura_temporal,
                'North Macedonia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MLI' as chave,
                '' as cobertura_temporal,
                'Mali' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MLT' as chave,
                '' as cobertura_temporal,
                'Malta' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MMR' as chave,
                '' as cobertura_temporal,
                'Myanmar' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MNE' as chave,
                '' as cobertura_temporal,
                'Montenegro' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MNG' as chave,
                '' as cobertura_temporal,
                'Mongolia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MOZ' as chave,
                '' as cobertura_temporal,
                'Mozambique' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MRT' as chave,
                '' as cobertura_temporal,
                'Mauritania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MSR' as chave,
                '' as cobertura_temporal,
                'Montserrat' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MUS' as chave,
                '' as cobertura_temporal,
                'Mauritius' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MWI' as chave,
                '' as cobertura_temporal,
                'Malawi' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'MYS' as chave,
                '' as cobertura_temporal,
                'Malaysia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NAM' as chave,
                '' as cobertura_temporal,
                'Namibia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NER' as chave,
                '' as cobertura_temporal,
                'Niger' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NGA' as chave,
                '' as cobertura_temporal,
                'Nigeria' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NIC' as chave,
                '' as cobertura_temporal,
                'Nicaragua' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NIU' as chave,
                '' as cobertura_temporal,
                'Niue' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NPL' as chave,
                '' as cobertura_temporal,
                'Nepal' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NRU' as chave,
                '' as cobertura_temporal,
                'Nauru' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Oceania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'OMN' as chave,
                '' as cobertura_temporal,
                'Oman' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'O_X' as chave,
                '' as cobertura_temporal,
                'Oceania unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PAK' as chave,
                '' as cobertura_temporal,
                'Pakistan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PAN' as chave,
                '' as cobertura_temporal,
                'Panama' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PHL' as chave,
                '' as cobertura_temporal,
                'Philippines' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PLW' as chave,
                '' as cobertura_temporal,
                'Palau' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PNG' as chave,
                '' as cobertura_temporal,
                'Papua New Guinea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PRI' as chave,
                '' as cobertura_temporal,
                'Puerto Rico' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PRK' as chave,
                '' as cobertura_temporal,
                'Democratic People’s Republic of Korea' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PRY' as chave,
                '' as cobertura_temporal,
                'Paraguay' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'PSE' as chave,
                '' as cobertura_temporal,
                'Palestinian Authority or West Bank and Gaza Strip' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'QAT' as chave,
                '' as cobertura_temporal,
                'Qatar' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'RWA' as chave,
                '' as cobertura_temporal,
                'Rwanda' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'S' as chave,
                '' as cobertura_temporal,
                'Asia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SDN' as chave,
                '' as cobertura_temporal,
                'Sudan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SEN' as chave,
                '' as cobertura_temporal,
                'Senegal' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SGP' as chave,
                '' as cobertura_temporal,
                'Singapore' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SLB' as chave,
                '' as cobertura_temporal,
                'Solomon Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SLE' as chave,
                '' as cobertura_temporal,
                'Sierra Leone' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SLV' as chave,
                '' as cobertura_temporal,
                'El Salvador' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SMR' as chave,
                '' as cobertura_temporal,
                'San Marino' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SOM' as chave,
                '' as cobertura_temporal,
                'Somalia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SRB' as chave,
                '' as cobertura_temporal,
                'Serbia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SSD' as chave,
                '' as cobertura_temporal,
                'South Sudan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'STP' as chave,
                '' as cobertura_temporal,
                'Sao Tome and Principe' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SUR' as chave,
                '' as cobertura_temporal,
                'Suriname' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SWZ' as chave,
                '' as cobertura_temporal,
                'Eswatini' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SXM' as chave,
                '' as cobertura_temporal,
                'Sint Maarten' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SYC' as chave,
                '' as cobertura_temporal,
                'Seychelles' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'SYR' as chave,
                '' as cobertura_temporal,
                'Syrian Arab Republic' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'S_X' as chave,
                '' as cobertura_temporal,
                'Asia unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TCA' as chave,
                '' as cobertura_temporal,
                'Turks and Caicos Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TCD' as chave,
                '' as cobertura_temporal,
                'Chad' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TGO' as chave,
                '' as cobertura_temporal,
                'Togo' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TJK' as chave,
                '' as cobertura_temporal,
                'Tajikistan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TKL' as chave,
                '' as cobertura_temporal,
                'Tokelau' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TKM' as chave,
                '' as cobertura_temporal,
                'Turkmenistan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TLS' as chave,
                '' as cobertura_temporal,
                'Timor-Leste' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TON' as chave,
                '' as cobertura_temporal,
                'Tonga' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TTO' as chave,
                '' as cobertura_temporal,
                'Trinidad and Tobago' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TUN' as chave,
                '' as cobertura_temporal,
                'Tunisia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TUV' as chave,
                '' as cobertura_temporal,
                'Tuvalu' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'TZA' as chave,
                '' as cobertura_temporal,
                'Tanzania' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'UGA' as chave,
                '' as cobertura_temporal,
                'Uganda' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'UKR' as chave,
                '' as cobertura_temporal,
                'Ukraine' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'URY' as chave,
                '' as cobertura_temporal,
                'Uruguay' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'UZB' as chave,
                '' as cobertura_temporal,
                'Uzbekistan' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VAT' as chave,
                '' as cobertura_temporal,
                'Holy See' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VCT' as chave,
                '' as cobertura_temporal,
                'Saint Vincent and the Grenadines' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VEN' as chave,
                '' as cobertura_temporal,
                'Venezuela' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VGB' as chave,
                '' as cobertura_temporal,
                'British Virgin Islands' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VNM' as chave,
                '' as cobertura_temporal,
                'Viet Nam' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'VUT' as chave,
                '' as cobertura_temporal,
                'Vanuatu' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'World' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'WSM' as chave,
                '' as cobertura_temporal,
                'Samoa' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'W_O_X' as chave,
                '' as cobertura_temporal,
                'Other countries unspecified' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'XKV' as chave,
                '' as cobertura_temporal,
                'Kosovo' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'YEM' as chave,
                '' as cobertura_temporal,
                'Yemen' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ZMB' as chave,
                '' as cobertura_temporal,
                'Zambia' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                'ZWE' as chave,
                '' as cobertura_temporal,
                'Zimbabwe' as valor
            ),
            struct(
                'student' as id_tabela,
                'destination_area' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU' as chave,
                '' as cobertura_temporal,
                'All educational institutions' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_GOV' as chave,
                '' as cobertura_temporal,
                'Government dependent private educational institutions' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_IND' as chave,
                '' as cobertura_temporal,
                'Independent private educational institutions' as valor
            ),
            struct(
                'student' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'student' as id_tabela,
                'mobility' as nome_coluna,
                'MOB' as chave,
                '' as cobertura_temporal,
                'Mobile including homecoming nationals' as valor
            ),
            struct(
                'student' as id_tabela,
                'mobility' as nome_coluna,
                'NMOB' as chave,
                '' as cobertura_temporal,
                'Non-mobile' as valor
            ),
            struct(
                'student' as id_tabela,
                'mobility' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_measure' as nome_coluna,
                'PS' as chave,
                '' as cobertura_temporal,
                'Persons' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_POP_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of population in the same subgroup' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_ST' as chave,
                '' as cobertura_temporal,
                'Percentage of students' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_ST_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of students in the same subgroup' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_measure' as nome_coluna,
                'Y' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'student' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'student' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'student' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y0' as chave,
                '' as cobertura_temporal,
                '0 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y1' as chave,
                '' as cobertura_temporal,
                '1 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y10' as chave,
                '' as cobertura_temporal,
                '10 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y11' as chave,
                '' as cobertura_temporal,
                '11 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y12' as chave,
                '' as cobertura_temporal,
                '12 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y13' as chave,
                '' as cobertura_temporal,
                '13 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y14' as chave,
                '' as cobertura_temporal,
                '14 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y15' as chave,
                '' as cobertura_temporal,
                '15 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y15T19' as chave,
                '' as cobertura_temporal,
                'From 15 to 19 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y16' as chave,
                '' as cobertura_temporal,
                '16 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y17' as chave,
                '' as cobertura_temporal,
                '17 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y18' as chave,
                '' as cobertura_temporal,
                '18 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y19' as chave,
                '' as cobertura_temporal,
                '19 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y2' as chave,
                '' as cobertura_temporal,
                '2 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y20' as chave,
                '' as cobertura_temporal,
                '20 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y20T24' as chave,
                '' as cobertura_temporal,
                'From 20 to 24 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y21' as chave,
                '' as cobertura_temporal,
                '21 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y22' as chave,
                '' as cobertura_temporal,
                '22 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y23' as chave,
                '' as cobertura_temporal,
                '23 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y24' as chave,
                '' as cobertura_temporal,
                '24 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y25' as chave,
                '' as cobertura_temporal,
                '25 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y25T29' as chave,
                '' as cobertura_temporal,
                'From 25 to 29 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y26' as chave,
                '' as cobertura_temporal,
                '26 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y27' as chave,
                '' as cobertura_temporal,
                '27 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y28' as chave,
                '' as cobertura_temporal,
                '28 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y29' as chave,
                '' as cobertura_temporal,
                '29 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y3' as chave,
                '' as cobertura_temporal,
                '3 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y30' as chave,
                '' as cobertura_temporal,
                '30 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y30T34' as chave,
                '' as cobertura_temporal,
                'From 30 to 34 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y31' as chave,
                '' as cobertura_temporal,
                '31 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y32' as chave,
                '' as cobertura_temporal,
                '32 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y33' as chave,
                '' as cobertura_temporal,
                '33 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y34' as chave,
                '' as cobertura_temporal,
                '34 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y35' as chave,
                '' as cobertura_temporal,
                '35 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y35T39' as chave,
                '' as cobertura_temporal,
                'From 35 to 39 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y36' as chave,
                '' as cobertura_temporal,
                '36 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y37' as chave,
                '' as cobertura_temporal,
                '37 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y38' as chave,
                '' as cobertura_temporal,
                '38 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y39' as chave,
                '' as cobertura_temporal,
                '39 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y3T5' as chave,
                '' as cobertura_temporal,
                'From 3 to 5 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y4' as chave,
                '' as cobertura_temporal,
                '4 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y40' as chave,
                '' as cobertura_temporal,
                '40 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y40T44' as chave,
                '' as cobertura_temporal,
                'From 40 to 44 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y41' as chave,
                '' as cobertura_temporal,
                '41 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y42' as chave,
                '' as cobertura_temporal,
                '42 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y43' as chave,
                '' as cobertura_temporal,
                '43 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y44' as chave,
                '' as cobertura_temporal,
                '44 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y45' as chave,
                '' as cobertura_temporal,
                '45 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y45T49' as chave,
                '' as cobertura_temporal,
                'From 45 to 49 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y46' as chave,
                '' as cobertura_temporal,
                '46 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y47' as chave,
                '' as cobertura_temporal,
                '47 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y48' as chave,
                '' as cobertura_temporal,
                '48 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y49' as chave,
                '' as cobertura_temporal,
                '49 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y5' as chave,
                '' as cobertura_temporal,
                '5 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y50T54' as chave,
                '' as cobertura_temporal,
                'From 50 to 54 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y55T59' as chave,
                '' as cobertura_temporal,
                'From 55 to 59 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y6' as chave,
                '' as cobertura_temporal,
                '6 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y60T64' as chave,
                '' as cobertura_temporal,
                'From 60 to 64 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y6T14' as chave,
                '' as cobertura_temporal,
                'From 6 to 14 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y7' as chave,
                '' as cobertura_temporal,
                '7 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y8' as chave,
                '' as cobertura_temporal,
                '8 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y9' as chave,
                '' as cobertura_temporal,
                '9 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_GE65' as chave,
                '' as cobertura_temporal,
                '65 years or over' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_GT24' as chave,
                '' as cobertura_temporal,
                'Over 24 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_GT49' as chave,
                '' as cobertura_temporal,
                'Over 49 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_LT10' as chave,
                '' as cobertura_temporal,
                'Less than 10 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_LT15' as chave,
                '' as cobertura_temporal,
                'Less than 15 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_LT2' as chave,
                '' as cobertura_temporal,
                'Less than 2 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_LT3' as chave,
                '' as cobertura_temporal,
                'Less than 3 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                'Y_LT4' as chave,
                '' as cobertura_temporal,
                'Less than 4 years' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'student' as id_tabela,
                'age' as nome_coluna,
                '_U' as chave,
                '' as cobertura_temporal,
                'Unknown' as valor
            ),
            struct(
                'student' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'student' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'student' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'student' as id_tabela,
                'obs_status' as nome_coluna,
                'K' as chave,
                '' as cobertura_temporal,
                'Data included in another category' as valor
            ),
            struct(
                'student' as id_tabela,
                'obs_status' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Missing value; data cannot exist' as valor
            ),
            struct(
                'student' as id_tabela,
                'obs_status' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Missing value' as valor
            ),
            struct(
                'student' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'student' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'G20' as chave,
                '' as cobertura_temporal,
                'G20' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_0' as chave,
                '' as cobertura_temporal,
                'Early childhood education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_01' as chave,
                '' as cobertura_temporal,
                'Early childhood educational development' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_44' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_45' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_6T8' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s, Master\'s and Doctoral or equivalent level' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'measure' as nome_coluna,
                'AIDE' as chave,
                '' as cobertura_temporal,
                'Teacher aides (ISCED 0-3)' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'measure' as nome_coluna,
                'CLS' as chave,
                '' as cobertura_temporal,
                'Student classes' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'measure' as nome_coluna,
                'STU_PERS' as chave,
                '' as cobertura_temporal,
                'Students adjusted to personnel' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'measure' as nome_coluna,
                'TEACH' as chave,
                '' as cobertura_temporal,
                'Classroom teachers (ISCED 0-4) and academic staff (ISCED 5-8)' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_type' as nome_coluna,
                'FE' as chave,
                '' as cobertura_temporal,
                'Formal education, includes formal initial and formal adult education programmes'
                as valor
            ),
            struct(
                'teacher' as id_tabela,
                'intensity' as nome_coluna,
                'FT' as chave,
                '' as cobertura_temporal,
                'Full-time' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'intensity' as nome_coluna,
                'PT' as chave,
                '' as cobertura_temporal,
                'Part-time' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'intensity' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_field' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'grade' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'frequency' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Annual' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'origin_area' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'destination_area' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU' as chave,
                '' as cobertura_temporal,
                'All educational institutions' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_GOV' as chave,
                '' as cobertura_temporal,
                'Government dependent private educational institutions' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_IND' as chave,
                '' as cobertura_temporal,
                'Independent private educational institutions' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'mobility' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'PS' as chave,
                '' as cobertura_temporal,
                'Persons' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'PS_FTE' as chave,
                '' as cobertura_temporal,
                'Persons (full-time equivalent)' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_ST' as chave,
                '' as cobertura_temporal,
                'Percentage of students' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_ST_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of students in the same subgroup' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'ST_CL' as chave,
                '' as cobertura_temporal,
                'Students per class' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'ST_TCHR' as chave,
                '' as cobertura_temporal,
                'Students per teacher' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'sex' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y25T29' as chave,
                '' as cobertura_temporal,
                'From 25 to 29 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y30T34' as chave,
                '' as cobertura_temporal,
                'From 30 to 34 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y30T49' as chave,
                '' as cobertura_temporal,
                'From 30 to 49 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y35T39' as chave,
                '' as cobertura_temporal,
                'From 35 to 39 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y40T44' as chave,
                '' as cobertura_temporal,
                'From 40 to 44 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y45T49' as chave,
                '' as cobertura_temporal,
                'From 45 to 49 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y50T54' as chave,
                '' as cobertura_temporal,
                'From 50 to 54 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y55T59' as chave,
                '' as cobertura_temporal,
                'From 55 to 59 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y60T64' as chave,
                '' as cobertura_temporal,
                'From 60 to 64 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_GE50' as chave,
                '' as cobertura_temporal,
                '50 years or over' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_GE65' as chave,
                '' as cobertura_temporal,
                '65 years or over' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_LT25' as chave,
                '' as cobertura_temporal,
                'Less than 25 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_LT30' as chave,
                '' as cobertura_temporal,
                'Less than 30 years' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                '_U' as chave,
                '' as cobertura_temporal,
                'Unknown' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'age' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'K' as chave,
                '' as cobertura_temporal,
                'Data included in another category' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Missing value; data cannot exist' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Missing value' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'teacher' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CHN' as chave,
                '' as cobertura_temporal,
                'China (People’s Republic of)' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'IDN' as chave,
                '' as cobertura_temporal,
                'Indonesia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'IND' as chave,
                '' as cobertura_temporal,
                'India' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'finance' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'finance' as id_tabela,
                'measure' as nome_coluna,
                'EXP' as chave,
                '' as cobertura_temporal,
                'Expenditure on education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'measure' as nome_coluna,
                'FIN_PERSTUD' as chave,
                '' as cobertura_temporal,
                'Expenditure per full-time equivalent student' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_0' as chave,
                '' as cobertura_temporal,
                'Early childhood education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_01' as chave,
                '' as cobertura_temporal,
                'Early childhood educational development' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T4' as chave,
                '' as cobertura_temporal,
                'Primary to post-secondary non-tertiary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T8' as chave,
                '' as cobertura_temporal,
                'Primary to tertiary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_25' as chave,
                '' as cobertura_temporal,
                'Lower secondary vocational education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2_3' as chave,
                '' as cobertura_temporal,
                'Secondary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34_44' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary general programmes'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35_45' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary vocational programmes'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3_4' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary all programmes'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_44' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_45' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_54T84' as chave,
                '' as cobertura_temporal,
                'Tertiary general/academic education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_55T85' as chave,
                '' as cobertura_temporal,
                'Tertiary vocational/professional education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_6T8' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s, Master\'s and Doctoral or equivalent level' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_9' as chave,
                '' as cobertura_temporal,
                'Not elsewhere classified' as valor
            ),
            struct(
                'finance' as id_tabela,
                'education_level' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S13' as chave,
                '' as cobertura_temporal,
                'General government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S1311' as chave,
                '' as cobertura_temporal,
                'Central government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S1312' as chave,
                '' as cobertura_temporal,
                'State government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S1313' as chave,
                '' as cobertura_temporal,
                'Local government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S14' as chave,
                '' as cobertura_temporal,
                'Households' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S1D_NON_EDU' as chave,
                '' as cobertura_temporal,
                'Private sector (households and other non-educational private entities)'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S1D_NON_EDU_O' as chave,
                '' as cobertura_temporal,
                'Other non-educational private sector' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                'S2' as chave,
                '' as cobertura_temporal,
                'Rest of the world' as valor
            ),
            struct(
                'finance' as id_tabela,
                'financing_source' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU' as chave,
                '' as cobertura_temporal,
                'All educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU_PRIV_GOV' as chave,
                '' as cobertura_temporal,
                'Government dependent private educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU_PRIV_IND' as chave,
                '' as cobertura_temporal,
                'Independent private educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_NON_EDU' as chave,
                '' as cobertura_temporal,
                'Non-educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S13' as chave,
                '' as cobertura_temporal,
                'General government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S1311' as chave,
                '' as cobertura_temporal,
                'Central government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S1312' as chave,
                '' as cobertura_temporal,
                'State government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S1313' as chave,
                '' as cobertura_temporal,
                'Local government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S13M' as chave,
                '' as cobertura_temporal,
                'State and local government' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S14' as chave,
                '' as cobertura_temporal,
                'Households' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S1D_NON_EDU' as chave,
                '' as cobertura_temporal,
                'Private sector (households and other non-educational private entities)'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'S1D_NON_EDU_O' as chave,
                '' as cobertura_temporal,
                'Other non-educational private sector' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_destination' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'ADJ' as chave,
                '' as cobertura_temporal,
                'Adjustments for changes in fund balances' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'ASERV' as chave,
                '' as cobertura_temporal,
                'Expenditure for ancillary services' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CAP' as chave,
                '' as cobertura_temporal,
                'Capital expenditure' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CORE' as chave,
                '' as cobertura_temporal,
                'Expenditure for core services' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR' as chave,
                '' as cobertura_temporal,
                'Current expenditure' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_COMP' as chave,
                '' as cobertura_temporal,
                'Current expenditure on staff compensation (teaching and non-teaching staff)'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_COMPO' as chave,
                '' as cobertura_temporal,
                'Current expenditure for compensation of non-teaching staff' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_COMPT' as chave,
                '' as cobertura_temporal,
                'Current expenditure for compensation of teaching staff' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_LC' as chave,
                '' as cobertura_temporal,
                'Current expenditure for salaries' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_NLC' as chave,
                '' as cobertura_temporal,
                'Current expenditure for other non-salary compensation' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_O' as chave,
                '' as cobertura_temporal,
                'Current expenditure other than for staff compensation' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'CUR_RET' as chave,
                '' as cobertura_temporal,
                'Current expenditure for retirement pensions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'DIR_EXP' as chave,
                '' as cobertura_temporal,
                'Expenditure for educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'ED_ACT' as chave,
                '' as cobertura_temporal,
                'Payments for specific educational activities' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'FAHS' as chave,
                '' as cobertura_temporal,
                'Financial aid to households and students' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'FINAL' as chave,
                '' as cobertura_temporal,
                'Final expenditure (after transfers)' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'GD_DIR' as chave,
                '' as cobertura_temporal,
                'Payments on goods requested directly or indirectly by educational institutions'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'GD_IND' as chave,
                '' as cobertura_temporal,
                'Payments on goods not directly needed for participation' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'GRNT' as chave,
                '' as cobertura_temporal,
                'Scholarships and other grants to students/households' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'GRNT_TF' as chave,
                '' as cobertura_temporal,
                'Public grants attributable for tuition fees to educational institutions'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'INITIAL' as chave,
                '' as cobertura_temporal,
                'Initial expenditure (before transfers)' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'NET_TRF' as chave,
                '' as cobertura_temporal,
                'Intergovernmental transfers for education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'NORD' as chave,
                '' as cobertura_temporal,
                'Excluding research and development (R&D)' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'PAY' as chave,
                '' as cobertura_temporal,
                'Total household payments for tutoring and other goods and services outside educational institutions'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'RD' as chave,
                '' as cobertura_temporal,
                'Expenditure for R&D in educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'STU_LOAN' as chave,
                '' as cobertura_temporal,
                'Student loans' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'STU_LOAN_TF' as chave,
                '' as cobertura_temporal,
                'Student loans attributable for tuition fees to educational institutions'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'TRF' as chave,
                '' as cobertura_temporal,
                'Transfers from non-domestic sources for education to government'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'TRF_PAY' as chave,
                '' as cobertura_temporal,
                'Transfers and payments for education to the non-education private sector'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                'TUT' as chave,
                '' as cobertura_temporal,
                'Payments for private tutoring' as valor
            ),
            struct(
                'finance' as id_tabela,
                'expenditure_type' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total - all expenditure types' as valor
            ),
            struct(
                'finance' as id_tabela,
                'price_base' as nome_coluna,
                'Q' as chave,
                '' as cobertura_temporal,
                'Constant prices' as valor
            ),
            struct(
                'finance' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'finance' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_B1GQ' as chave,
                '' as cobertura_temporal,
                'Percentage of GDP' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_B1GQ_POP' as chave,
                '' as cobertura_temporal,
                'Percentage of GDP per capita' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_EXP' as chave,
                '' as cobertura_temporal,
                'Percentage of expenditure' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_EXP_CUR_EDU_INST' as chave,
                '' as cobertura_temporal,
                'Percentage of current expenditure on educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_EXP_EDU_INST' as chave,
                '' as cobertura_temporal,
                'Percentage of expenditure on educational institutions' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_OTE_S13' as chave,
                '' as cobertura_temporal,
                'Percentage of general government expenditure' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_OTE_S13_EDU' as chave,
                '' as cobertura_temporal,
                'Percentage of general government expenditure on education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP' as chave,
                '' as cobertura_temporal,
                'US dollars, PPP converted' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP_ST' as chave,
                '' as cobertura_temporal,
                'US dollars per student, PPP converted' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC_ST' as chave,
                '' as cobertura_temporal,
                'National currency per student' as valor
            ),
            struct(
                'finance' as id_tabela,
                'questionnaire_sheet' as nome_coluna,
                'NATURE' as chave,
                '' as cobertura_temporal,
                'FIN2-NATURE - Nature of expenditure in educational institutions'
                as valor
            ),
            struct(
                'finance' as id_tabela,
                'questionnaire_sheet' as nome_coluna,
                'SOURCE' as chave,
                '' as cobertura_temporal,
                'FIN1-SOURCE - Source of expenditure on education' as valor
            ),
            struct(
                'finance' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'finance' as id_tabela,
                'decimals' as nome_coluna,
                '1' as chave,
                '' as cobertura_temporal,
                'One' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'K' as chave,
                '' as cobertura_temporal,
                'Data included in another category' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'L' as chave,
                '' as cobertura_temporal,
                'Missing value; data exist but were not collected' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Missing value; data cannot exist' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'R' as chave,
                '' as cobertura_temporal,
                'Excludes one or more subcategories' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status_2' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status_2' as nome_coluna,
                'L' as chave,
                '' as cobertura_temporal,
                'Missing value; data exist but were not collected' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status_2' as nome_coluna,
                'R' as chave,
                '' as cobertura_temporal,
                'Excludes one or more subcategories' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status_2' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'finance' as id_tabela,
                'obs_status_3' as nome_coluna,
                'B' as chave,
                '' as cobertura_temporal,
                'Time series break' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'finance' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '6' as chave,
                '' as cobertura_temporal,
                'Millions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CHN' as chave,
                '' as cobertura_temporal,
                'China (People’s Republic of)' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'IDN' as chave,
                '' as cobertura_temporal,
                'Indonesia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'IND' as chave,
                '' as cobertura_temporal,
                'India' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'measure' as nome_coluna,
                'ENR' as chave,
                '' as cobertura_temporal,
                'Students enrolled' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_0' as chave,
                '' as cobertura_temporal,
                'Early childhood education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_01' as chave,
                '' as cobertura_temporal,
                'Early childhood educational development' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T4' as chave,
                '' as cobertura_temporal,
                'Primary to post-secondary non-tertiary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T8' as chave,
                '' as cobertura_temporal,
                'Primary to tertiary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_25' as chave,
                '' as cobertura_temporal,
                'Lower secondary vocational education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2_3' as chave,
                '' as cobertura_temporal,
                'Secondary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34_44' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary general programmes'
                as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35_45' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary vocational programmes'
                as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3_4' as chave,
                '' as cobertura_temporal,
                'Upper secondary and post-secondary non-tertiary all programmes'
                as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_44' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_45' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_54T84' as chave,
                '' as cobertura_temporal,
                'Tertiary general/academic education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_55T85' as chave,
                '' as cobertura_temporal,
                'Tertiary vocational/professional education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_6T8' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s, Master\'s and Doctoral or equivalent level' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_9' as chave,
                '' as cobertura_temporal,
                'Not elsewhere classified' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_level' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'intensity' as nome_coluna,
                'FT' as chave,
                '' as cobertura_temporal,
                'Full-time' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'intensity' as nome_coluna,
                'FTE' as chave,
                '' as cobertura_temporal,
                'Full-time equivalent' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'intensity' as nome_coluna,
                'PT' as chave,
                '' as cobertura_temporal,
                'Part-time' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU' as chave,
                '' as cobertura_temporal,
                'All educational institutions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_GOV' as chave,
                '' as cobertura_temporal,
                'Government dependent private educational institutions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PRIV_IND' as chave,
                '' as cobertura_temporal,
                'Independent private educational institutions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'unit_measure' as nome_coluna,
                'PS' as chave,
                '' as cobertura_temporal,
                'Persons' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'questionnaire_sheet' as nome_coluna,
                'STUDENTS' as chave,
                '' as cobertura_temporal,
                'FIN-STUDENTS - Number of students adjusted to the financial year'
                as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status' as nome_coluna,
                'L' as chave,
                '' as cobertura_temporal,
                'Missing value; data exist but were not collected' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status' as nome_coluna,
                'R' as chave,
                '' as cobertura_temporal,
                'Excludes one or more subcategories' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status_2' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'obs_status_3' as nome_coluna,
                'B' as chave,
                '' as cobertura_temporal,
                'Time series break' as valor
            ),
            struct(
                'finance_enrolment' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CHN' as chave,
                '' as cobertura_temporal,
                'China (People’s Republic of)' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'IDN' as chave,
                '' as cobertura_temporal,
                'Indonesia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'IND' as chave,
                '' as cobertura_temporal,
                'India' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'GDP' as chave,
                '' as cobertura_temporal,
                'GDP' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'GDP_ADJ' as chave,
                '' as cobertura_temporal,
                'GDP - adjusted' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'GDP_CAPITA' as chave,
                '' as cobertura_temporal,
                'GDP per capita' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'GDP_DEFLATOR' as chave,
                '' as cobertura_temporal,
                'GDP deflator' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'MULTIPLIER' as chave,
                '' as cobertura_temporal,
                'Unit multiplier' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'POP' as chave,
                '' as cobertura_temporal,
                'Population' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'PPPGDP' as chave,
                '' as cobertura_temporal,
                'Purchasing Power Parities for GDP' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'measure' as nome_coluna,
                'T_PUB_EXP' as chave,
                '' as cobertura_temporal,
                'Government expenditure on all services' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'price_base' as nome_coluna,
                'D' as chave,
                '' as cobertura_temporal,
                'Deflator' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'price_base' as nome_coluna,
                'Q' as chave,
                '' as cobertura_temporal,
                'Constant prices' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'IX' as chave,
                '' as cobertura_temporal,
                'Index' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'PS' as chave,
                '' as cobertura_temporal,
                'Persons' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_B1GQ' as chave,
                '' as cobertura_temporal,
                'Percentage of GDP' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP_PS' as chave,
                '' as cobertura_temporal,
                'US dollars per person, PPP converted' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC_PS' as chave,
                '' as cobertura_temporal,
                'National currency per person' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC_USD' as chave,
                '' as cobertura_temporal,
                'National currency per US dollar' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_measure' as nome_coluna,
                '_X' as chave,
                '' as cobertura_temporal,
                'Unspecified' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'questionnaire_sheet' as nome_coluna,
                'ANNEX' as chave,
                '' as cobertura_temporal,
                'External reference statistics (e.g. GDP, total government expenditure)'
                as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'decimals' as nome_coluna,
                '1' as chave,
                '' as cobertura_temporal,
                'One' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'obs_status_3' as nome_coluna,
                'B' as chave,
                '' as cobertura_temporal,
                'Time series break' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '3' as chave,
                '' as cobertura_temporal,
                'Thousands' as valor
            ),
            struct(
                'finance_reference' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '6' as chave,
                '' as cobertura_temporal,
                'Millions' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA10' as chave,
                '' as cobertura_temporal,
                'Newfoundland and Labrador' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA11' as chave,
                '' as cobertura_temporal,
                'Prince Edward Island' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA12' as chave,
                '' as cobertura_temporal,
                'Nova Scotia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA13' as chave,
                '' as cobertura_temporal,
                'New Brunswick' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA24' as chave,
                '' as cobertura_temporal,
                'Quebec' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA35' as chave,
                '' as cobertura_temporal,
                'Ontario' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA46' as chave,
                '' as cobertura_temporal,
                'Manitoba' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA47' as chave,
                '' as cobertura_temporal,
                'Saskatchewan' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA59' as chave,
                '' as cobertura_temporal,
                'British Columbia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA60' as chave,
                '' as cobertura_temporal,
                'Yukon' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA61' as chave,
                '' as cobertura_temporal,
                'Northwest Territories' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CA62' as chave,
                '' as cobertura_temporal,
                'Nunavut' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH01' as chave,
                '' as cobertura_temporal,
                'Lake Geneva Region' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH02' as chave,
                '' as cobertura_temporal,
                'Espace Mittelland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH03' as chave,
                '' as cobertura_temporal,
                'Northwestern Switzerland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH04' as chave,
                '' as cobertura_temporal,
                'Zurich' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH05' as chave,
                '' as cobertura_temporal,
                'Eastern Switzerland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH06' as chave,
                '' as cobertura_temporal,
                'Central Switzerland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'CH07' as chave,
                '' as cobertura_temporal,
                'Ticino' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE1' as chave,
                '' as cobertura_temporal,
                'Baden-Württemberg' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE2' as chave,
                '' as cobertura_temporal,
                'Bavaria' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE3' as chave,
                '' as cobertura_temporal,
                'Berlin' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE4' as chave,
                '' as cobertura_temporal,
                'Brandenburg' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE5' as chave,
                '' as cobertura_temporal,
                'Bremen' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE6' as chave,
                '' as cobertura_temporal,
                'Hamburg' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE7' as chave,
                '' as cobertura_temporal,
                'Hesse' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE8' as chave,
                '' as cobertura_temporal,
                'Mecklenburg-Vorpommern' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DE9' as chave,
                '' as cobertura_temporal,
                'Lower Saxony' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEA' as chave,
                '' as cobertura_temporal,
                'North Rhine-Westphalia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEB' as chave,
                '' as cobertura_temporal,
                'Rhineland-Palatinate' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEC' as chave,
                '' as cobertura_temporal,
                'Saarland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DED' as chave,
                '' as cobertura_temporal,
                'Saxony' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEE' as chave,
                '' as cobertura_temporal,
                'Saxony-Anhalt' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEF' as chave,
                '' as cobertura_temporal,
                'Schleswig-Holstein' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'DEG' as chave,
                '' as cobertura_temporal,
                'Thuringia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES11' as chave,
                '' as cobertura_temporal,
                'Galicia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES12' as chave,
                '' as cobertura_temporal,
                'Asturias' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES13' as chave,
                '' as cobertura_temporal,
                'Cantabria' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES21' as chave,
                '' as cobertura_temporal,
                'Basque Country' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES22' as chave,
                '' as cobertura_temporal,
                'Navarra' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES23' as chave,
                '' as cobertura_temporal,
                'La Rioja' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES24' as chave,
                '' as cobertura_temporal,
                'Aragon' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES30' as chave,
                '' as cobertura_temporal,
                'Madrid' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES41' as chave,
                '' as cobertura_temporal,
                'Castile and León' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES42' as chave,
                '' as cobertura_temporal,
                'Castile-La Mancha' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES43' as chave,
                '' as cobertura_temporal,
                'Extremadura' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES51' as chave,
                '' as cobertura_temporal,
                'Catalonia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES52' as chave,
                '' as cobertura_temporal,
                'Valencian Community' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES53' as chave,
                '' as cobertura_temporal,
                'Balearic Islands' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES61' as chave,
                '' as cobertura_temporal,
                'Andalusia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES62' as chave,
                '' as cobertura_temporal,
                'Murcia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'ES70' as chave,
                '' as cobertura_temporal,
                'Canary Islands' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'LT01' as chave,
                '' as cobertura_temporal,
                'Capital Region' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'LT02' as chave,
                '' as cobertura_temporal,
                'Central and Western Lithuania Region' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US01' as chave,
                '' as cobertura_temporal,
                'Alabama' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US02' as chave,
                '' as cobertura_temporal,
                'Alaska' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US04' as chave,
                '' as cobertura_temporal,
                'Arizona' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US05' as chave,
                '' as cobertura_temporal,
                'Arkansas' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US06' as chave,
                '' as cobertura_temporal,
                'California' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US08' as chave,
                '' as cobertura_temporal,
                'Colorado' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US09' as chave,
                '' as cobertura_temporal,
                'Connecticut' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US10' as chave,
                '' as cobertura_temporal,
                'Delaware' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US11' as chave,
                '' as cobertura_temporal,
                'District of Columbia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US12' as chave,
                '' as cobertura_temporal,
                'Florida' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US13' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US15' as chave,
                '' as cobertura_temporal,
                'Hawaii' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US16' as chave,
                '' as cobertura_temporal,
                'Idaho' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US17' as chave,
                '' as cobertura_temporal,
                'Illinois' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US18' as chave,
                '' as cobertura_temporal,
                'Indiana' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US19' as chave,
                '' as cobertura_temporal,
                'Iowa' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US20' as chave,
                '' as cobertura_temporal,
                'Kansas' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US21' as chave,
                '' as cobertura_temporal,
                'Kentucky' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US22' as chave,
                '' as cobertura_temporal,
                'Louisiana' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US23' as chave,
                '' as cobertura_temporal,
                'Maine' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US24' as chave,
                '' as cobertura_temporal,
                'Maryland' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US25' as chave,
                '' as cobertura_temporal,
                'Massachusetts' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US26' as chave,
                '' as cobertura_temporal,
                'Michigan' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US27' as chave,
                '' as cobertura_temporal,
                'Minnesota' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US28' as chave,
                '' as cobertura_temporal,
                'Mississippi' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US29' as chave,
                '' as cobertura_temporal,
                'Missouri' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US30' as chave,
                '' as cobertura_temporal,
                'Montana' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US31' as chave,
                '' as cobertura_temporal,
                'Nebraska' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US32' as chave,
                '' as cobertura_temporal,
                'Nevada' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US33' as chave,
                '' as cobertura_temporal,
                'New Hampshire' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US34' as chave,
                '' as cobertura_temporal,
                'New Jersey' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US35' as chave,
                '' as cobertura_temporal,
                'New Mexico' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US36' as chave,
                '' as cobertura_temporal,
                'New York' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US37' as chave,
                '' as cobertura_temporal,
                'North Carolina' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US38' as chave,
                '' as cobertura_temporal,
                'North Dakota' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US39' as chave,
                '' as cobertura_temporal,
                'Ohio' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US40' as chave,
                '' as cobertura_temporal,
                'Oklahoma' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US41' as chave,
                '' as cobertura_temporal,
                'Oregon' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US42' as chave,
                '' as cobertura_temporal,
                'Pennsylvania' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US44' as chave,
                '' as cobertura_temporal,
                'Rhode Island' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US45' as chave,
                '' as cobertura_temporal,
                'South Carolina' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US46' as chave,
                '' as cobertura_temporal,
                'South Dakota' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US47' as chave,
                '' as cobertura_temporal,
                'Tennessee' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US48' as chave,
                '' as cobertura_temporal,
                'Texas' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US49' as chave,
                '' as cobertura_temporal,
                'Utah' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US50' as chave,
                '' as cobertura_temporal,
                'Vermont' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US51' as chave,
                '' as cobertura_temporal,
                'Virginia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US53' as chave,
                '' as cobertura_temporal,
                'Washington' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US54' as chave,
                '' as cobertura_temporal,
                'West Virginia' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US55' as chave,
                '' as cobertura_temporal,
                'Wisconsin' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'reference_area' as nome_coluna,
                'US56' as chave,
                '' as cobertura_temporal,
                'Wyoming' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'measure' as nome_coluna,
                'FIN_PERSTUD' as chave,
                '' as cobertura_temporal,
                'Expenditure per full-time equivalent student' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T3' as chave,
                '' as cobertura_temporal,
                'Primary and secondary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1T8' as chave,
                '' as cobertura_temporal,
                'Primary to tertiary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_6T8' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s, Master\'s and Doctoral or equivalent level' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'financing_source' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'expenditure_destination' as nome_coluna,
                'INST_EDU' as chave,
                '' as cobertura_temporal,
                'All educational institutions' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'expenditure_type' as nome_coluna,
                'DIR_EXP' as chave,
                '' as cobertura_temporal,
                'Expenditure for educational institutions' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'expenditure_type' as nome_coluna,
                'NORD' as chave,
                '' as cobertura_temporal,
                'Excluding research and development (R&D)' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP_ST' as chave,
                '' as cobertura_temporal,
                'US dollars per student, PPP converted' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'finance_subnational' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CHN' as chave,
                '' as cobertura_temporal,
                'China (People’s Republic of)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'G20' as chave,
                '' as cobertura_temporal,
                'G20' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'GBR' as chave,
                '' as cobertura_temporal,
                'United Kingdom' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'IDN' as chave,
                '' as cobertura_temporal,
                'Indonesia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'IND' as chave,
                '' as cobertura_temporal,
                'India' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'RUS' as chave,
                '' as cobertura_temporal,
                'Russia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'age' as nome_coluna,
                'Y25T34' as chave,
                '' as cobertura_temporal,
                'From 25 to 34 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'age' as nome_coluna,
                'Y25T64' as chave,
                '' as cobertura_temporal,
                'From 25 to 64 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'age' as nome_coluna,
                'Y35T44' as chave,
                '' as cobertura_temporal,
                'From 35 to 44 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'age' as nome_coluna,
                'Y45T54' as chave,
                '' as cobertura_temporal,
                'From 45 to 54 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'age' as nome_coluna,
                'Y55T64' as chave,
                '' as cobertura_temporal,
                'From 55 to 64 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_0' as chave,
                '' as cobertura_temporal,
                'Less than primary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_0T2' as chave,
                '' as cobertura_temporal,
                'Below upper secondary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_242_252' as chave,
                '' as cobertura_temporal,
                'Lower secondary education, partial level completion and without direct access to upper secondary'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_342_352' as chave,
                '' as cobertura_temporal,
                'Upper secondary education, partial level completion and without direct access to tertiary education'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_34_44' as chave,
                '' as cobertura_temporal,
                'General upper secondary or post-secondary non-tertiary education'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_35_45' as chave,
                '' as cobertura_temporal,
                'Vocational upper secondary or post-secondary non-tertiary education'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_3_4' as chave,
                '' as cobertura_temporal,
                'Upper secondary or post-secondary non-tertiary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_3_4_X' as chave,
                '' as cobertura_temporal,
                'Upper secondary or post-secondary non-tertiary education, orientation not specified'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_3_X' as chave,
                '' as cobertura_temporal,
                'Upper secondary education (orientation not specified)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_4' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_44' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary general education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_45' as chave,
                '' as cobertura_temporal,
                'Post-secondary non-tertiary vocational education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_4_X' as chave,
                '' as cobertura_temporal,
                'Post secondary non-tertiary education (orientation not specified)'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_5' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_54' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary general education, sufficient for level completion'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_55' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary vocational education, sufficient for level completion'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_5T8' as chave,
                '' as cobertura_temporal,
                'Tertiary education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_5_X' as chave,
                '' as cobertura_temporal,
                'Short-cycle tertiary education (orientation not specified)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_6' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s or equivalent level' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_6T8' as chave,
                '' as cobertura_temporal,
                'Bachelor\'s, master\'s, doctoral or equivalent level' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_7' as chave,
                '' as cobertura_temporal,
                'Master’s or equivalent level' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_7_8' as chave,
                '' as cobertura_temporal,
                'Master\'s, doctoral or equivalent level' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                'ISCED11A_8' as chave,
                '' as cobertura_temporal,
                'Doctoral or equivalent level' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'attainment_level' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F00' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F00_08_10' as chave,
                '' as cobertura_temporal,
                'Generic programmes and qualifications; agriculture, forestry, fisheries and veterinary; services'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F01' as chave,
                '' as cobertura_temporal,
                'Education (broad field level)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F02' as chave,
                '' as cobertura_temporal,
                'Arts and humanities' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F021' as chave,
                '' as cobertura_temporal,
                'Arts' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F022' as chave,
                '' as cobertura_temporal,
                'Humanities (except languages)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F022T03' as chave,
                '' as cobertura_temporal,
                'Humanities (except languages), social sciences, journalism and information'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F02_03' as chave,
                '' as cobertura_temporal,
                'Arts, humanities, social sciences, journalism and information' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F03' as chave,
                '' as cobertura_temporal,
                'Social sciences, journalism and information' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F04' as chave,
                '' as cobertura_temporal,
                'Business, administration and law' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F041' as chave,
                '' as cobertura_temporal,
                'Business and administration' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F042' as chave,
                '' as cobertura_temporal,
                'Law (narrow field level)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F05' as chave,
                '' as cobertura_temporal,
                'Natural sciences, mathematics and statistics' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F05T07' as chave,
                '' as cobertura_temporal,
                'Science, technology, engineering and mathematics' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F06' as chave,
                '' as cobertura_temporal,
                'Information and Communication Technologies (ICTs) (broad field level)'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F07' as chave,
                '' as cobertura_temporal,
                'Engineering, manufacturing and construction' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F08' as chave,
                '' as cobertura_temporal,
                'Agriculture, forestry, fisheries and veterinary' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F09' as chave,
                '' as cobertura_temporal,
                'Health and welfare (broad field level)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F0911_0912' as chave,
                '' as cobertura_temporal,
                'Health (medical & dental)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F0913T0917' as chave,
                '' as cobertura_temporal,
                'Health (nursing and associate health fields)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                'F10' as chave,
                '' as cobertura_temporal,
                'Services (broad field level)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_field' as nome_coluna,
                '_TX_U' as chave,
                '' as cobertura_temporal,
                'Total (excluding unknown)' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'measure' as nome_coluna,
                'EMPLOYMENT' as chave,
                '' as cobertura_temporal,
                'Employment rate' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'measure' as nome_coluna,
                'INACTIVITY' as chave,
                '' as cobertura_temporal,
                'Inactivity rate' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'measure' as nome_coluna,
                'LF_PARTICIPATION' as chave,
                '' as cobertura_temporal,
                'Labour force participation rate' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'measure' as nome_coluna,
                'POP' as chave,
                '' as cobertura_temporal,
                'Population' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'measure' as nome_coluna,
                'UNEMPLOYMENT' as chave,
                '' as cobertura_temporal,
                'Unemployment rate' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'income' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'birth_place' as nome_coluna,
                'FB' as chave,
                '' as cobertura_temporal,
                'Foreign-born' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'birth_place' as nome_coluna,
                'NB' as chave,
                '' as cobertura_temporal,
                'Native-born' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'birth_place' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'birth_place' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'migration_age' as nome_coluna,
                'Y_GE16' as chave,
                '' as cobertura_temporal,
                '16 years or over' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'migration_age' as nome_coluna,
                'Y_LT16' as chave,
                '' as cobertura_temporal,
                'Less than 16 years' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'migration_age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'migration_age' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'education_status' as nome_coluna,
                'ED_NED' as chave,
                '' as cobertura_temporal,
                'In education or not in education' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'labour_force_status' as nome_coluna,
                'EMP' as chave,
                '' as cobertura_temporal,
                'Employment' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'labour_force_status' as nome_coluna,
                'LF' as chave,
                '' as cobertura_temporal,
                'Labour force' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'labour_force_status' as nome_coluna,
                'OLF' as chave,
                '' as cobertura_temporal,
                'Outside the labour force' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'labour_force_status' as nome_coluna,
                'POP' as chave,
                '' as cobertura_temporal,
                'Population' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'labour_force_status' as nome_coluna,
                'UNE' as chave,
                '' as cobertura_temporal,
                'Unemployment' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                'M3T11' as chave,
                '' as cobertura_temporal,
                '3-11 months' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                'M_GE12' as chave,
                '' as cobertura_temporal,
                '12 months or more' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                'M_LE12' as chave,
                '' as cobertura_temporal,
                'Less than 12 months' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                'M_LT3' as chave,
                '' as cobertura_temporal,
                'Less than 3 months' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unemployment_duration' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_LF_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of labour force in the same subgroup' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_POP_SEX_AGE' as chave,
                '' as cobertura_temporal,
                'Percentage of population in the same sex and age' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_POP_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of population in the same subgroup' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_UNEMPT_SUB' as chave,
                '' as cobertura_temporal,
                'Percentage of unemployment in the same subgroup' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'statistical_operation' as nome_coluna,
                'SE' as chave,
                '' as cobertura_temporal,
                'Standard error' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'work_time_arrangement' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'questionnaire' as nome_coluna,
                'NEAC' as chave,
                '' as cobertura_temporal,
                'LSO-NEAC regular data collection' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'questionnaire' as nome_coluna,
                'NEAC_DURUNEMP' as chave,
                '' as cobertura_temporal,
                'LSO-NEAC cyclical data collection on duration of unemployment' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'questionnaire' as nome_coluna,
                'NEAC_FIELD' as chave,
                '' as cobertura_temporal,
                'LSO-NEAC cyclical data collection on field of study' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'questionnaire' as nome_coluna,
                'NEAC_MIGR' as chave,
                '' as cobertura_temporal,
                'LSO-NEAC cyclical data collection on country of birth and age at migration'
                as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'frequency' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Annual' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'frequency' as nome_coluna,
                'A3' as chave,
                '' as cobertura_temporal,
                'Triennial' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'confidentiality_status' as nome_coluna,
                'C' as chave,
                '' as cobertura_temporal,
                'Confidential statistical information' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'decimals' as nome_coluna,
                '1' as chave,
                '' as cobertura_temporal,
                'One' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'obs_status' as nome_coluna,
                'B' as chave,
                '' as cobertura_temporal,
                'Time series break' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'obs_status' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Missing value' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'obs_status' as nome_coluna,
                'U' as chave,
                '' as cobertura_temporal,
                'Low reliability' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'obs_status' as nome_coluna,
                'W' as chave,
                '' as cobertura_temporal,
                'Includes data from another category' as valor
            ),
            struct(
                'labour_market_outcome' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'BFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'BFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKB' as chave,
                '' as cobertura_temporal,
                'England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKC' as chave,
                '' as cobertura_temporal,
                'North East England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKD' as chave,
                '' as cobertura_temporal,
                'North West England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKE' as chave,
                '' as cobertura_temporal,
                'Yorkshire and The Humber' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKF' as chave,
                '' as cobertura_temporal,
                'East Midlands' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKG' as chave,
                '' as cobertura_temporal,
                'West Midlands' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKH' as chave,
                '' as cobertura_temporal,
                'East of England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKI' as chave,
                '' as cobertura_temporal,
                'Greater London' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKJ' as chave,
                '' as cobertura_temporal,
                'South East England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKK' as chave,
                '' as cobertura_temporal,
                'South West England' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKL' as chave,
                '' as cobertura_temporal,
                'Wales' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'UKN' as chave,
                '' as cobertura_temporal,
                'Northern Ireland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US01' as chave,
                '' as cobertura_temporal,
                'Alabama' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US02' as chave,
                '' as cobertura_temporal,
                'Alaska' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US04' as chave,
                '' as cobertura_temporal,
                'Arizona' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US05' as chave,
                '' as cobertura_temporal,
                'Arkansas' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US06' as chave,
                '' as cobertura_temporal,
                'California' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US08' as chave,
                '' as cobertura_temporal,
                'Colorado' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US09' as chave,
                '' as cobertura_temporal,
                'Connecticut' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US10' as chave,
                '' as cobertura_temporal,
                'Delaware' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US11' as chave,
                '' as cobertura_temporal,
                'District of Columbia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US12' as chave,
                '' as cobertura_temporal,
                'Florida' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US13' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US15' as chave,
                '' as cobertura_temporal,
                'Hawaii' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US16' as chave,
                '' as cobertura_temporal,
                'Idaho' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US17' as chave,
                '' as cobertura_temporal,
                'Illinois' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US18' as chave,
                '' as cobertura_temporal,
                'Indiana' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US19' as chave,
                '' as cobertura_temporal,
                'Iowa' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US20' as chave,
                '' as cobertura_temporal,
                'Kansas' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US21' as chave,
                '' as cobertura_temporal,
                'Kentucky' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US22' as chave,
                '' as cobertura_temporal,
                'Louisiana' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US23' as chave,
                '' as cobertura_temporal,
                'Maine' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US24' as chave,
                '' as cobertura_temporal,
                'Maryland' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US25' as chave,
                '' as cobertura_temporal,
                'Massachusetts' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US26' as chave,
                '' as cobertura_temporal,
                'Michigan' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US27' as chave,
                '' as cobertura_temporal,
                'Minnesota' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US28' as chave,
                '' as cobertura_temporal,
                'Mississippi' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US29' as chave,
                '' as cobertura_temporal,
                'Missouri' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US30' as chave,
                '' as cobertura_temporal,
                'Montana' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US31' as chave,
                '' as cobertura_temporal,
                'Nebraska' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US32' as chave,
                '' as cobertura_temporal,
                'Nevada' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US33' as chave,
                '' as cobertura_temporal,
                'New Hampshire' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US34' as chave,
                '' as cobertura_temporal,
                'New Jersey' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US35' as chave,
                '' as cobertura_temporal,
                'New Mexico' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US36' as chave,
                '' as cobertura_temporal,
                'New York' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US37' as chave,
                '' as cobertura_temporal,
                'North Carolina' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US38' as chave,
                '' as cobertura_temporal,
                'North Dakota' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US39' as chave,
                '' as cobertura_temporal,
                'Ohio' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US40' as chave,
                '' as cobertura_temporal,
                'Oklahoma' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US41' as chave,
                '' as cobertura_temporal,
                'Oregon' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US42' as chave,
                '' as cobertura_temporal,
                'Pennsylvania' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US44' as chave,
                '' as cobertura_temporal,
                'Rhode Island' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US45' as chave,
                '' as cobertura_temporal,
                'South Carolina' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US46' as chave,
                '' as cobertura_temporal,
                'South Dakota' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US47' as chave,
                '' as cobertura_temporal,
                'Tennessee' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US48' as chave,
                '' as cobertura_temporal,
                'Texas' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US49' as chave,
                '' as cobertura_temporal,
                'Utah' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US50' as chave,
                '' as cobertura_temporal,
                'Vermont' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US51' as chave,
                '' as cobertura_temporal,
                'Virginia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US53' as chave,
                '' as cobertura_temporal,
                'Washington' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US54' as chave,
                '' as cobertura_temporal,
                'West Virginia' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US55' as chave,
                '' as cobertura_temporal,
                'Wisconsin' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'US56' as chave,
                '' as cobertura_temporal,
                'Wyoming' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'measure' as nome_coluna,
                'SAL_ACT' as chave,
                '' as cobertura_temporal,
                'Actual salaries' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'measure' as nome_coluna,
                'SAL_ACT_REL_SIM' as chave,
                '' as cobertura_temporal,
                'Actual salaries relative to earnings of similarly educated workers'
                as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'measure' as nome_coluna,
                'SAL_ACT_REL_TER' as chave,
                '' as cobertura_temporal,
                'Actual salaries relative to earnings of tertiary-educated workers'
                as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_EARN_FT_SIM' as chave,
                '' as cobertura_temporal,
                'Factor of earnings for full-time, full-year similarly educated workers'
                as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_EARN_FT_TER' as chave,
                '' as cobertura_temporal,
                'Factor of earnings for full-time, full-year tertiary-educated workers'
                as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP' as chave,
                '' as cobertura_temporal,
                'US dollars, PPP converted' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'age' as nome_coluna,
                'Y25T34' as chave,
                '' as cobertura_temporal,
                'From 25 to 34 years' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'age' as nome_coluna,
                'Y25T64' as chave,
                '' as cobertura_temporal,
                'From 25 to 64 years' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'age' as nome_coluna,
                'Y35T44' as chave,
                '' as cobertura_temporal,
                'From 35 to 44 years' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'age' as nome_coluna,
                'Y45T54' as chave,
                '' as cobertura_temporal,
                'From 45 to 54 years' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'age' as nome_coluna,
                'Y55T64' as chave,
                '' as cobertura_temporal,
                'From 55 to 64 years' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'personnel_type' as nome_coluna,
                'SH' as chave,
                '' as cobertura_temporal,
                'School head' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'personnel_type' as nome_coluna,
                'TE' as chave,
                '' as cobertura_temporal,
                'Teacher' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'AUD' as chave,
                '' as cobertura_temporal,
                'Australian dollar' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'CLP' as chave,
                '' as cobertura_temporal,
                'Chilean peso' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'CRC' as chave,
                '' as cobertura_temporal,
                'Costa Rican colon' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'CZK' as chave,
                '' as cobertura_temporal,
                'Czech koruna' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'DKK' as chave,
                '' as cobertura_temporal,
                'Danish krone' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'EUR' as chave,
                '' as cobertura_temporal,
                'Euro' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'GBP' as chave,
                '' as cobertura_temporal,
                'Pound sterling' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'HUF' as chave,
                '' as cobertura_temporal,
                'Forint' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'ILS' as chave,
                '' as cobertura_temporal,
                'New Israeli sheqel' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'ISK' as chave,
                '' as cobertura_temporal,
                'Iceland krona' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'KRW' as chave,
                '' as cobertura_temporal,
                'Won' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'NOK' as chave,
                '' as cobertura_temporal,
                'Norwegian krone' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'NZD' as chave,
                '' as cobertura_temporal,
                'New Zealand dollar' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'PEN' as chave,
                '' as cobertura_temporal,
                'Peruvian sol' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'PLN' as chave,
                '' as cobertura_temporal,
                'Zloty' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'RON' as chave,
                '' as cobertura_temporal,
                'Romanian leu' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'SEK' as chave,
                '' as cobertura_temporal,
                'Swedish krona' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'TRY' as chave,
                '' as cobertura_temporal,
                'Turkish lira' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                'USD' as chave,
                '' as cobertura_temporal,
                'US dollar' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'currency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'price_base' as nome_coluna,
                'Q' as chave,
                '' as cobertura_temporal,
                'Constant prices' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MEAN' as chave,
                '' as cobertura_temporal,
                'Arithmetic mean' as valor
            ),
            struct(
                'salary_actual' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA10' as chave,
                '' as cobertura_temporal,
                'Newfoundland and Labrador' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA11' as chave,
                '' as cobertura_temporal,
                'Prince Edward Island' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA12' as chave,
                '' as cobertura_temporal,
                'Nova Scotia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA13' as chave,
                '' as cobertura_temporal,
                'New Brunswick' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA24' as chave,
                '' as cobertura_temporal,
                'Quebec' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA35' as chave,
                '' as cobertura_temporal,
                'Ontario' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA46' as chave,
                '' as cobertura_temporal,
                'Manitoba' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA47' as chave,
                '' as cobertura_temporal,
                'Saskatchewan' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA59' as chave,
                '' as cobertura_temporal,
                'British Columbia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CA61' as chave,
                '' as cobertura_temporal,
                'Northwest Territories' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKB' as chave,
                '' as cobertura_temporal,
                'England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKC' as chave,
                '' as cobertura_temporal,
                'North East England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKD' as chave,
                '' as cobertura_temporal,
                'North West England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKE' as chave,
                '' as cobertura_temporal,
                'Yorkshire and The Humber' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKF' as chave,
                '' as cobertura_temporal,
                'East Midlands' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKG' as chave,
                '' as cobertura_temporal,
                'West Midlands' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKH' as chave,
                '' as cobertura_temporal,
                'East of England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKI' as chave,
                '' as cobertura_temporal,
                'Greater London' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKJ' as chave,
                '' as cobertura_temporal,
                'South East England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKK' as chave,
                '' as cobertura_temporal,
                'South West England' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKL' as chave,
                '' as cobertura_temporal,
                'Wales' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'UKN' as chave,
                '' as cobertura_temporal,
                'Northern Ireland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US01' as chave,
                '' as cobertura_temporal,
                'Alabama' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US02' as chave,
                '' as cobertura_temporal,
                'Alaska' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US04' as chave,
                '' as cobertura_temporal,
                'Arizona' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US05' as chave,
                '' as cobertura_temporal,
                'Arkansas' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US06' as chave,
                '' as cobertura_temporal,
                'California' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US08' as chave,
                '' as cobertura_temporal,
                'Colorado' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US09' as chave,
                '' as cobertura_temporal,
                'Connecticut' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US10' as chave,
                '' as cobertura_temporal,
                'Delaware' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US12' as chave,
                '' as cobertura_temporal,
                'Florida' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US13' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US15' as chave,
                '' as cobertura_temporal,
                'Hawaii' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US16' as chave,
                '' as cobertura_temporal,
                'Idaho' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US17' as chave,
                '' as cobertura_temporal,
                'Illinois' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US18' as chave,
                '' as cobertura_temporal,
                'Indiana' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US19' as chave,
                '' as cobertura_temporal,
                'Iowa' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US20' as chave,
                '' as cobertura_temporal,
                'Kansas' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US21' as chave,
                '' as cobertura_temporal,
                'Kentucky' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US22' as chave,
                '' as cobertura_temporal,
                'Louisiana' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US23' as chave,
                '' as cobertura_temporal,
                'Maine' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US24' as chave,
                '' as cobertura_temporal,
                'Maryland' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US25' as chave,
                '' as cobertura_temporal,
                'Massachusetts' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US26' as chave,
                '' as cobertura_temporal,
                'Michigan' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US27' as chave,
                '' as cobertura_temporal,
                'Minnesota' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US28' as chave,
                '' as cobertura_temporal,
                'Mississippi' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US29' as chave,
                '' as cobertura_temporal,
                'Missouri' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US30' as chave,
                '' as cobertura_temporal,
                'Montana' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US31' as chave,
                '' as cobertura_temporal,
                'Nebraska' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US32' as chave,
                '' as cobertura_temporal,
                'Nevada' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US34' as chave,
                '' as cobertura_temporal,
                'New Jersey' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US35' as chave,
                '' as cobertura_temporal,
                'New Mexico' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US36' as chave,
                '' as cobertura_temporal,
                'New York' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US37' as chave,
                '' as cobertura_temporal,
                'North Carolina' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US38' as chave,
                '' as cobertura_temporal,
                'North Dakota' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US39' as chave,
                '' as cobertura_temporal,
                'Ohio' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US40' as chave,
                '' as cobertura_temporal,
                'Oklahoma' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US41' as chave,
                '' as cobertura_temporal,
                'Oregon' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US42' as chave,
                '' as cobertura_temporal,
                'Pennsylvania' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US44' as chave,
                '' as cobertura_temporal,
                'Rhode Island' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US45' as chave,
                '' as cobertura_temporal,
                'South Carolina' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US46' as chave,
                '' as cobertura_temporal,
                'South Dakota' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US47' as chave,
                '' as cobertura_temporal,
                'Tennessee' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US48' as chave,
                '' as cobertura_temporal,
                'Texas' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US49' as chave,
                '' as cobertura_temporal,
                'Utah' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US50' as chave,
                '' as cobertura_temporal,
                'Vermont' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US51' as chave,
                '' as cobertura_temporal,
                'Virginia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US53' as chave,
                '' as cobertura_temporal,
                'Washington' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US54' as chave,
                '' as cobertura_temporal,
                'West Virginia' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US55' as chave,
                '' as cobertura_temporal,
                'Wisconsin' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'US56' as chave,
                '' as cobertura_temporal,
                'Wyoming' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA' as chave,
                '' as cobertura_temporal,
                'Statutory salaries' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_COM_ISCED' as chave,
                '' as cobertura_temporal,
                'Ratio of salary per net teaching hour of teachers in upper secondary general education to teachers in primary education'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_COM_MINMAX' as chave,
                '' as cobertura_temporal,
                'Ratio of maximum salary to minimum salary' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_COM_PERHR' as chave,
                '' as cobertura_temporal,
                'Statutory salary per hour of net teaching time for teachers' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_COM_RANGE' as chave,
                '' as cobertura_temporal,
                'Ratio of salary at the top of salary scale to salary at the start of the career'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_COM_YRTOP' as chave,
                '' as cobertura_temporal,
                'Years to reach salary at the top of salary scale from the start of the career'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_REL_SIM' as chave,
                '' as cobertura_temporal,
                'Statutory salaries relative to earnings of similarly educated workers'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA_REL_TER' as chave,
                '' as cobertura_temporal,
                'Statutory salaries relative to earnings of tertiary-educated workers'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_EARN_FT_SIM' as chave,
                '' as cobertura_temporal,
                'Factor of earnings for full-time, full-year similarly educated workers'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_EARN_FT_TER' as chave,
                '' as cobertura_temporal,
                'Factor of earnings for full-time, full-year tertiary-educated workers'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_SAL_EXP0' as chave,
                '' as cobertura_temporal,
                'Factor of salary at the start of the career' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_SAL_ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Factor of salary per net teaching hour of teachers in primary education'
                as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'FCTR_SAL_MIN' as chave,
                '' as cobertura_temporal,
                'Factor of minimum salary' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'USD_PPP' as chave,
                '' as cobertura_temporal,
                'US dollars, PPP converted' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_measure' as nome_coluna,
                'Y' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MAX' as chave,
                '' as cobertura_temporal,
                'Maximum' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MEAN' as chave,
                '' as cobertura_temporal,
                'Arithmetic mean' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MIN' as chave,
                '' as cobertura_temporal,
                'Minimum' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_type' as nome_coluna,
                'SH' as chave,
                '' as cobertura_temporal,
                'School head' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_type' as nome_coluna,
                'TE' as chave,
                '' as cobertura_temporal,
                'Teacher' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'MAX' as chave,
                '' as cobertura_temporal,
                'Maximum qualification' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'MAX_EXP' as chave,
                '' as cobertura_temporal,
                'Maximum qualification at this stage of career' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'MIN' as chave,
                '' as cobertura_temporal,
                'Minimum qualification' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'MIN_EXP' as chave,
                '' as cobertura_temporal,
                'Minimum qualification at this stage of career' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'TYP' as chave,
                '' as cobertura_temporal,
                'Most prevalent qualification' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'TYP_EXP' as chave,
                '' as cobertura_temporal,
                'Most prevalent qualification at this stage of career' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXP0' as chave,
                '' as cobertura_temporal,
                'At the start of the career' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXP10' as chave,
                '' as cobertura_temporal,
                'After 10 years of experience' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXP15' as chave,
                '' as cobertura_temporal,
                'After 15 years of experience' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXPMAX' as chave,
                '' as cobertura_temporal,
                'At top of the salary scale' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'All levels of experience' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'AUD' as chave,
                '' as cobertura_temporal,
                'Australian dollar' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'BGN' as chave,
                '' as cobertura_temporal,
                'Bulgarian lev' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'BRL' as chave,
                '' as cobertura_temporal,
                'Brazilian real' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'CAD' as chave,
                '' as cobertura_temporal,
                'Canadian dollar' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'CHF' as chave,
                '' as cobertura_temporal,
                'Swiss franc' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'CLP' as chave,
                '' as cobertura_temporal,
                'Chilean peso' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'COP' as chave,
                '' as cobertura_temporal,
                'Colombian peso' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'CRC' as chave,
                '' as cobertura_temporal,
                'Costa Rican colon' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'CZK' as chave,
                '' as cobertura_temporal,
                'Czech koruna' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'DKK' as chave,
                '' as cobertura_temporal,
                'Danish krone' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'EUR' as chave,
                '' as cobertura_temporal,
                'Euro' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'GBP' as chave,
                '' as cobertura_temporal,
                'Pound sterling' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'HUF' as chave,
                '' as cobertura_temporal,
                'Forint' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'ILS' as chave,
                '' as cobertura_temporal,
                'New Israeli sheqel' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'ISK' as chave,
                '' as cobertura_temporal,
                'Iceland krona' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'JPY' as chave,
                '' as cobertura_temporal,
                'Yen' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'KRW' as chave,
                '' as cobertura_temporal,
                'Won' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'MXN' as chave,
                '' as cobertura_temporal,
                'Mexican peso' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'NOK' as chave,
                '' as cobertura_temporal,
                'Norwegian krone' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'NZD' as chave,
                '' as cobertura_temporal,
                'New Zealand dollar' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'PEN' as chave,
                '' as cobertura_temporal,
                'Peruvian sol' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'PLN' as chave,
                '' as cobertura_temporal,
                'Zloty' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'RON' as chave,
                '' as cobertura_temporal,
                'Romanian leu' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'SEK' as chave,
                '' as cobertura_temporal,
                'Swedish krona' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'THB' as chave,
                '' as cobertura_temporal,
                'Thai bhat' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'TRY' as chave,
                '' as cobertura_temporal,
                'Turkish lira' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                'USD' as chave,
                '' as cobertura_temporal,
                'US dollar' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'currency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'obs_status' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Missing value; data cannot exist' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'obs_status' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Missing value' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'price_base' as nome_coluna,
                'Q' as chave,
                '' as cobertura_temporal,
                'Constant prices' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_statutory' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ARG' as chave,
                '' as cobertura_temporal,
                'Argentina' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD_REP' as chave,
                '' as cobertura_temporal,
                'OECD average country' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'UKB' as chave,
                '' as cobertura_temporal,
                'England' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'measure' as nome_coluna,
                'SAL_ACT' as chave,
                '' as cobertura_temporal,
                'Actual salaries' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'measure' as nome_coluna,
                'SAL_STA' as chave,
                '' as cobertura_temporal,
                'Statutory salaries' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'unit_measure' as nome_coluna,
                'IX' as chave,
                '' as cobertura_temporal,
                'Index' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'unit_measure' as nome_coluna,
                'XDC' as chave,
                '' as cobertura_temporal,
                'National currency' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'age' as nome_coluna,
                'Y25T64' as chave,
                '' as cobertura_temporal,
                'From 25 to 64 years' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_type' as nome_coluna,
                'TE' as chave,
                '' as cobertura_temporal,
                'Teacher' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                'TYP_EXP' as chave,
                '' as cobertura_temporal,
                'Most prevalent qualification at this stage of career' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_qualification_level' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXP0' as chave,
                '' as cobertura_temporal,
                'At the start of the career' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                'EXP15' as chave,
                '' as cobertura_temporal,
                'After 15 years of experience' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'personnel_experience_level' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'price_base' as nome_coluna,
                'V' as chave,
                '' as cobertura_temporal,
                'Current prices' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'price_base' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'ARS' as chave,
                '' as cobertura_temporal,
                'Argentine peso' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'AUD' as chave,
                '' as cobertura_temporal,
                'Australian dollar' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'BGN' as chave,
                '' as cobertura_temporal,
                'Bulgarian lev' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'BRL' as chave,
                '' as cobertura_temporal,
                'Brazilian real' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'CAD' as chave,
                '' as cobertura_temporal,
                'Canadian dollar' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'CHF' as chave,
                '' as cobertura_temporal,
                'Swiss franc' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'CLP' as chave,
                '' as cobertura_temporal,
                'Chilean peso' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'COP' as chave,
                '' as cobertura_temporal,
                'Colombian peso' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'CRC' as chave,
                '' as cobertura_temporal,
                'Costa Rican colon' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'CZK' as chave,
                '' as cobertura_temporal,
                'Czech koruna' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'DKK' as chave,
                '' as cobertura_temporal,
                'Danish krone' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'EUR' as chave,
                '' as cobertura_temporal,
                'Euro' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'HUF' as chave,
                '' as cobertura_temporal,
                'Forint' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'ILS' as chave,
                '' as cobertura_temporal,
                'New Israeli sheqel' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'ISK' as chave,
                '' as cobertura_temporal,
                'Iceland krona' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'JPY' as chave,
                '' as cobertura_temporal,
                'Yen' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'KRW' as chave,
                '' as cobertura_temporal,
                'Won' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'MXN' as chave,
                '' as cobertura_temporal,
                'Mexican peso' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'NOK' as chave,
                '' as cobertura_temporal,
                'Norwegian krone' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'NZD' as chave,
                '' as cobertura_temporal,
                'New Zealand dollar' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'PEN' as chave,
                '' as cobertura_temporal,
                'Peruvian sol' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'PLN' as chave,
                '' as cobertura_temporal,
                'Zloty' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'RON' as chave,
                '' as cobertura_temporal,
                'Romanian leu' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'SAR' as chave,
                '' as cobertura_temporal,
                'Saudi riyal' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'SEK' as chave,
                '' as cobertura_temporal,
                'Swedish krona' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'TRY' as chave,
                '' as cobertura_temporal,
                'Turkish lira' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'USD' as chave,
                '' as cobertura_temporal,
                'US dollar' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                'ZAR' as chave,
                '' as cobertura_temporal,
                'Rand' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'currency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'obs_status' as nome_coluna,
                'B' as chave,
                '' as cobertura_temporal,
                'Time series break' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'obs_status' as nome_coluna,
                'L' as chave,
                '' as cobertura_temporal,
                'Missing value; data exist but were not collected' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'obs_status' as nome_coluna,
                'O' as chave,
                '' as cobertura_temporal,
                'Missing value' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MEAN' as chave,
                '' as cobertura_temporal,
                'Arithmetic mean' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'transformation' as nome_coluna,
                'MIX_100' as chave,
                '' as cobertura_temporal,
                'Multilateral index rebased to 100' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'transformation' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'salary_trend' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA10' as chave,
                '' as cobertura_temporal,
                'Newfoundland and Labrador' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA11' as chave,
                '' as cobertura_temporal,
                'Prince Edward Island' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA12' as chave,
                '' as cobertura_temporal,
                'Nova Scotia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA13' as chave,
                '' as cobertura_temporal,
                'New Brunswick' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA24' as chave,
                '' as cobertura_temporal,
                'Quebec' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA35' as chave,
                '' as cobertura_temporal,
                'Ontario' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA46' as chave,
                '' as cobertura_temporal,
                'Manitoba' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA47' as chave,
                '' as cobertura_temporal,
                'Saskatchewan' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA59' as chave,
                '' as cobertura_temporal,
                'British Columbia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA61' as chave,
                '' as cobertura_temporal,
                'Northwest Territories' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'EU22OECD' as chave,
                '' as cobertura_temporal,
                'European Union (22 countries) in OECD' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR011' as chave,
                '' as cobertura_temporal,
                'Seoul' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR012' as chave,
                '' as cobertura_temporal,
                'Incheon' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR013' as chave,
                '' as cobertura_temporal,
                'Gyeonggi-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR021' as chave,
                '' as cobertura_temporal,
                'Busan' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR022' as chave,
                '' as cobertura_temporal,
                'Ulsan' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR023' as chave,
                '' as cobertura_temporal,
                'Gyeongsangnam-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR031' as chave,
                '' as cobertura_temporal,
                'Daegu' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR032' as chave,
                '' as cobertura_temporal,
                'Gyeongsangbuk-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR041' as chave,
                '' as cobertura_temporal,
                'Gwangju' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR042' as chave,
                '' as cobertura_temporal,
                'Jeollabuk-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR043' as chave,
                '' as cobertura_temporal,
                'Jeollanam-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR051' as chave,
                '' as cobertura_temporal,
                'Daejeon' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR052' as chave,
                '' as cobertura_temporal,
                'Chungcheongbuk-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR053' as chave,
                '' as cobertura_temporal,
                'Chungcheongnam-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR054' as chave,
                '' as cobertura_temporal,
                'Sejong' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR061' as chave,
                '' as cobertura_temporal,
                'Gangwon-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KR071' as chave,
                '' as cobertura_temporal,
                'Jeju-do' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'THA' as chave,
                '' as cobertura_temporal,
                'Thailand' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'UKB' as chave,
                '' as cobertura_temporal,
                'England' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'UKL' as chave,
                '' as cobertura_temporal,
                'Wales' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US01' as chave,
                '' as cobertura_temporal,
                'Alabama' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US02' as chave,
                '' as cobertura_temporal,
                'Alaska' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US04' as chave,
                '' as cobertura_temporal,
                'Arizona' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US05' as chave,
                '' as cobertura_temporal,
                'Arkansas' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US06' as chave,
                '' as cobertura_temporal,
                'California' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US08' as chave,
                '' as cobertura_temporal,
                'Colorado' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US09' as chave,
                '' as cobertura_temporal,
                'Connecticut' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US10' as chave,
                '' as cobertura_temporal,
                'Delaware' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US11' as chave,
                '' as cobertura_temporal,
                'District of Columbia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US12' as chave,
                '' as cobertura_temporal,
                'Florida' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US13' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US15' as chave,
                '' as cobertura_temporal,
                'Hawaii' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US16' as chave,
                '' as cobertura_temporal,
                'Idaho' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US17' as chave,
                '' as cobertura_temporal,
                'Illinois' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US18' as chave,
                '' as cobertura_temporal,
                'Indiana' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US19' as chave,
                '' as cobertura_temporal,
                'Iowa' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US20' as chave,
                '' as cobertura_temporal,
                'Kansas' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US21' as chave,
                '' as cobertura_temporal,
                'Kentucky' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US22' as chave,
                '' as cobertura_temporal,
                'Louisiana' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US23' as chave,
                '' as cobertura_temporal,
                'Maine' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US24' as chave,
                '' as cobertura_temporal,
                'Maryland' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US25' as chave,
                '' as cobertura_temporal,
                'Massachusetts' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US26' as chave,
                '' as cobertura_temporal,
                'Michigan' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US27' as chave,
                '' as cobertura_temporal,
                'Minnesota' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US28' as chave,
                '' as cobertura_temporal,
                'Mississippi' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US29' as chave,
                '' as cobertura_temporal,
                'Missouri' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US30' as chave,
                '' as cobertura_temporal,
                'Montana' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US31' as chave,
                '' as cobertura_temporal,
                'Nebraska' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US32' as chave,
                '' as cobertura_temporal,
                'Nevada' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US33' as chave,
                '' as cobertura_temporal,
                'New Hampshire' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US34' as chave,
                '' as cobertura_temporal,
                'New Jersey' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US35' as chave,
                '' as cobertura_temporal,
                'New Mexico' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US36' as chave,
                '' as cobertura_temporal,
                'New York' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US37' as chave,
                '' as cobertura_temporal,
                'North Carolina' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US38' as chave,
                '' as cobertura_temporal,
                'North Dakota' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US39' as chave,
                '' as cobertura_temporal,
                'Ohio' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US40' as chave,
                '' as cobertura_temporal,
                'Oklahoma' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US41' as chave,
                '' as cobertura_temporal,
                'Oregon' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US42' as chave,
                '' as cobertura_temporal,
                'Pennsylvania' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US44' as chave,
                '' as cobertura_temporal,
                'Rhode Island' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US45' as chave,
                '' as cobertura_temporal,
                'South Carolina' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US46' as chave,
                '' as cobertura_temporal,
                'South Dakota' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US47' as chave,
                '' as cobertura_temporal,
                'Tennessee' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US48' as chave,
                '' as cobertura_temporal,
                'Texas' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US49' as chave,
                '' as cobertura_temporal,
                'Utah' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US50' as chave,
                '' as cobertura_temporal,
                'Vermont' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US51' as chave,
                '' as cobertura_temporal,
                'Virginia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US53' as chave,
                '' as cobertura_temporal,
                'Washington' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US54' as chave,
                '' as cobertura_temporal,
                'West Virginia' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US55' as chave,
                '' as cobertura_temporal,
                'Wisconsin' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US56' as chave,
                '' as cobertura_temporal,
                'Wyoming' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_ACT_TCH' as chave,
                '' as cobertura_temporal,
                'Actual teaching time' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_TCH' as chave,
                '' as cobertura_temporal,
                'Statutory teaching time' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_TCH_MAX' as chave,
                '' as cobertura_temporal,
                'Statutory maximum teaching time' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_TCH_MIN' as chave,
                '' as cobertura_temporal,
                'Statutory minimum teaching time' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_WKS' as chave,
                '' as cobertura_temporal,
                'Statutory working time required at school' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_WKT' as chave,
                '' as cobertura_temporal,
                'Total statutory working time' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'D_Y' as chave,
                '' as cobertura_temporal,
                'Days per year' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'H_Y' as chave,
                '' as cobertura_temporal,
                'Hours per year' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'WK_Y' as chave,
                '' as cobertura_temporal,
                'Weeks per year' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_25' as chave,
                '' as cobertura_temporal,
                'Lower secondary vocational education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_35' as chave,
                '' as cobertura_temporal,
                'Upper secondary vocational education' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'personnel_type' as nome_coluna,
                'SH' as chave,
                '' as cobertura_temporal,
                'School head' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'personnel_type' as nome_coluna,
                'TE' as chave,
                '' as cobertura_temporal,
                'Teacher' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MAX' as chave,
                '' as cobertura_temporal,
                'Maximum' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MIN' as chave,
                '' as cobertura_temporal,
                'Minimum' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'working_time' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'RUS' as chave,
                '' as cobertura_temporal,
                'Russia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'measure' as nome_coluna,
                'WKT_STA_TCH' as chave,
                '' as cobertura_temporal,
                'Statutory teaching time' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'unit_measure' as nome_coluna,
                'H_Y' as chave,
                '' as cobertura_temporal,
                'Hours per year' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_02' as chave,
                '' as cobertura_temporal,
                'Pre-primary education' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'personnel_type' as nome_coluna,
                'TE' as chave,
                '' as cobertura_temporal,
                'Teacher' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'working_time_trend' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA10' as chave,
                '' as cobertura_temporal,
                'Newfoundland and Labrador' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA11' as chave,
                '' as cobertura_temporal,
                'Prince Edward Island' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA12' as chave,
                '' as cobertura_temporal,
                'Nova Scotia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA13' as chave,
                '' as cobertura_temporal,
                'New Brunswick' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA24' as chave,
                '' as cobertura_temporal,
                'Quebec' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA35' as chave,
                '' as cobertura_temporal,
                'Ontario' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA46' as chave,
                '' as cobertura_temporal,
                'Manitoba' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA47' as chave,
                '' as cobertura_temporal,
                'Saskatchewan' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA59' as chave,
                '' as cobertura_temporal,
                'British Columbia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CA61' as chave,
                '' as cobertura_temporal,
                'Northwest Territories' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CAN' as chave,
                '' as cobertura_temporal,
                'Canada' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CHE' as chave,
                '' as cobertura_temporal,
                'Switzerland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'DEU' as chave,
                '' as cobertura_temporal,
                'Germany' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES11' as chave,
                '' as cobertura_temporal,
                'Galicia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES12' as chave,
                '' as cobertura_temporal,
                'Asturias' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES13' as chave,
                '' as cobertura_temporal,
                'Cantabria' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES21' as chave,
                '' as cobertura_temporal,
                'Basque Country' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES22' as chave,
                '' as cobertura_temporal,
                'Navarra' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES23' as chave,
                '' as cobertura_temporal,
                'La Rioja' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES24' as chave,
                '' as cobertura_temporal,
                'Aragon' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES30' as chave,
                '' as cobertura_temporal,
                'Madrid' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES41' as chave,
                '' as cobertura_temporal,
                'Castile and León' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES42' as chave,
                '' as cobertura_temporal,
                'Castile-La Mancha' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES43' as chave,
                '' as cobertura_temporal,
                'Extremadura' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES51' as chave,
                '' as cobertura_temporal,
                'Catalonia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES52' as chave,
                '' as cobertura_temporal,
                'Valencia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES53' as chave,
                '' as cobertura_temporal,
                'Balearic Islands' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES61' as chave,
                '' as cobertura_temporal,
                'Andalusia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES62' as chave,
                '' as cobertura_temporal,
                'Murcia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES63' as chave,
                '' as cobertura_temporal,
                'Ceuta' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES64' as chave,
                '' as cobertura_temporal,
                'Melilla' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ES70' as chave,
                '' as cobertura_temporal,
                'Canary Islands' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'EU25' as chave,
                '' as cobertura_temporal,
                'European Union (25 countries)' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'GRC' as chave,
                '' as cobertura_temporal,
                'Greece' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'IRL' as chave,
                '' as cobertura_temporal,
                'Ireland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LUX' as chave,
                '' as cobertura_temporal,
                'Luxembourg' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'MEX' as chave,
                '' as cobertura_temporal,
                'Mexico' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'PER' as chave,
                '' as cobertura_temporal,
                'Peru' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'UKB' as chave,
                '' as cobertura_temporal,
                'England' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'UKM' as chave,
                '' as cobertura_temporal,
                'Scotland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US01' as chave,
                '' as cobertura_temporal,
                'Alabama' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US02' as chave,
                '' as cobertura_temporal,
                'Alaska' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US04' as chave,
                '' as cobertura_temporal,
                'Arizona' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US05' as chave,
                '' as cobertura_temporal,
                'Arkansas' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US06' as chave,
                '' as cobertura_temporal,
                'California' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US08' as chave,
                '' as cobertura_temporal,
                'Colorado' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US09' as chave,
                '' as cobertura_temporal,
                'Connecticut' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US10' as chave,
                '' as cobertura_temporal,
                'Delaware' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US11' as chave,
                '' as cobertura_temporal,
                'District of Columbia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US12' as chave,
                '' as cobertura_temporal,
                'Florida' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US13' as chave,
                '' as cobertura_temporal,
                'Georgia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US15' as chave,
                '' as cobertura_temporal,
                'Hawaii' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US16' as chave,
                '' as cobertura_temporal,
                'Idaho' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US17' as chave,
                '' as cobertura_temporal,
                'Illinois' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US18' as chave,
                '' as cobertura_temporal,
                'Indiana' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US19' as chave,
                '' as cobertura_temporal,
                'Iowa' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US20' as chave,
                '' as cobertura_temporal,
                'Kansas' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US21' as chave,
                '' as cobertura_temporal,
                'Kentucky' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US22' as chave,
                '' as cobertura_temporal,
                'Louisiana' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US23' as chave,
                '' as cobertura_temporal,
                'Maine' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US24' as chave,
                '' as cobertura_temporal,
                'Maryland' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US25' as chave,
                '' as cobertura_temporal,
                'Massachusetts' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US26' as chave,
                '' as cobertura_temporal,
                'Michigan' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US27' as chave,
                '' as cobertura_temporal,
                'Minnesota' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US28' as chave,
                '' as cobertura_temporal,
                'Mississippi' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US29' as chave,
                '' as cobertura_temporal,
                'Missouri' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US30' as chave,
                '' as cobertura_temporal,
                'Montana (US)' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US31' as chave,
                '' as cobertura_temporal,
                'Nebraska' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US32' as chave,
                '' as cobertura_temporal,
                'Nevada' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US33' as chave,
                '' as cobertura_temporal,
                'New Hampshire' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US34' as chave,
                '' as cobertura_temporal,
                'New Jersey' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US35' as chave,
                '' as cobertura_temporal,
                'New Mexico' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US36' as chave,
                '' as cobertura_temporal,
                'New York' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US37' as chave,
                '' as cobertura_temporal,
                'North Carolina' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US38' as chave,
                '' as cobertura_temporal,
                'North Dakota' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US39' as chave,
                '' as cobertura_temporal,
                'Ohio' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US40' as chave,
                '' as cobertura_temporal,
                'Oklahoma' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US41' as chave,
                '' as cobertura_temporal,
                'Oregon' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US42' as chave,
                '' as cobertura_temporal,
                'Pennsylvania' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US44' as chave,
                '' as cobertura_temporal,
                'Rhode Island' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US45' as chave,
                '' as cobertura_temporal,
                'South Carolina' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US46' as chave,
                '' as cobertura_temporal,
                'South Dakota' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US47' as chave,
                '' as cobertura_temporal,
                'Tennessee' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US48' as chave,
                '' as cobertura_temporal,
                'Texas' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US49' as chave,
                '' as cobertura_temporal,
                'Utah' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US50' as chave,
                '' as cobertura_temporal,
                'Vermont' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US51' as chave,
                '' as cobertura_temporal,
                'Virginia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US53' as chave,
                '' as cobertura_temporal,
                'Washington' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US54' as chave,
                '' as cobertura_temporal,
                'West Virginia' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US55' as chave,
                '' as cobertura_temporal,
                'Wisconsin' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'US56' as chave,
                '' as cobertura_temporal,
                'Wyoming' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'measure' as nome_coluna,
                'INT_AGE' as chave,
                '' as cobertura_temporal,
                'Theoretical starting age' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'measure' as nome_coluna,
                'INT_GRA' as chave,
                '' as cobertura_temporal,
                'Grades' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'measure' as nome_coluna,
                'INT_TIME' as chave,
                '' as cobertura_temporal,
                'Instruction time' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'D_Y' as chave,
                '' as cobertura_temporal,
                'Days per year' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'GRDS' as chave,
                '' as cobertura_temporal,
                'Grades' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'H' as chave,
                '' as cobertura_temporal,
                'Hours' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'H_Y' as chave,
                '' as cobertura_temporal,
                'Hours per year' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_INST_COM' as chave,
                '' as cobertura_temporal,
                'Percentage of compulsory instruction time' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_measure' as nome_coluna,
                'Y' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1_2' as chave,
                '' as cobertura_temporal,
                'Primary and lower secondary education (basic education)' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_24' as chave,
                '' as cobertura_temporal,
                'Lower secondary general education' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_34' as chave,
                '' as cobertura_temporal,
                'Upper secondary general education' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'education_level' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y10' as chave,
                '' as cobertura_temporal,
                '10 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y11' as chave,
                '' as cobertura_temporal,
                '11 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y12' as chave,
                '' as cobertura_temporal,
                '12 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y13' as chave,
                '' as cobertura_temporal,
                '13 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y14' as chave,
                '' as cobertura_temporal,
                '14 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y15' as chave,
                '' as cobertura_temporal,
                '15 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y16' as chave,
                '' as cobertura_temporal,
                '16 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y17' as chave,
                '' as cobertura_temporal,
                '17 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y18' as chave,
                '' as cobertura_temporal,
                '18 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y5' as chave,
                '' as cobertura_temporal,
                '5 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y6' as chave,
                '' as cobertura_temporal,
                '6 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y7' as chave,
                '' as cobertura_temporal,
                '7 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y8' as chave,
                '' as cobertura_temporal,
                '8 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                'Y9' as chave,
                '' as cobertura_temporal,
                '9 years' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'age' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'ARTS' as chave,
                '' as cobertura_temporal,
                'Arts' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'COMPUL' as chave,
                '' as cobertura_temporal,
                'Compulsory curriculum' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'FXTM' as chave,
                '' as cobertura_temporal,
                'Subjects with flexible timetable' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'ICTS' as chave,
                '' as cobertura_temporal,
                'Information and communication technologies (ICT)' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'INTEND' as chave,
                '' as cobertura_temporal,
                'Intended instruction time' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'MATH' as chave,
                '' as cobertura_temporal,
                'Mathematics' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'NCOMPUL' as chave,
                '' as cobertura_temporal,
                'Non compulsory curriculum' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'NSCI' as chave,
                '' as cobertura_temporal,
                'Natural sciences' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'OLAN' as chave,
                '' as cobertura_temporal,
                'Other languages' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'OSCH' as chave,
                '' as cobertura_temporal,
                'Flexible subjects chosen by schools' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'OSTU' as chave,
                '' as cobertura_temporal,
                'Options chosen by the students' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'OSUB' as chave,
                '' as cobertura_temporal,
                'Other subjects' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'PHED' as chave,
                '' as cobertura_temporal,
                'Physical education and health' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'READ' as chave,
                '' as cobertura_temporal,
                'Reading, writing and literature' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'RELI' as chave,
                '' as cobertura_temporal,
                'Religion/ Ethics/ Moral education' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'SLAN' as chave,
                '' as cobertura_temporal,
                'Second language' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'SSCI' as chave,
                '' as cobertura_temporal,
                'Social sciences' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'TECH' as chave,
                '' as cobertura_temporal,
                'Technology' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'subject' as nome_coluna,
                'VOCS' as chave,
                '' as cobertura_temporal,
                'Practical and vocational skills' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'decimals' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Zero' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'statistical_operation' as nome_coluna,
                'OBS' as chave,
                '' as cobertura_temporal,
                'Observed' as valor
            ),
            struct(
                'instruction_time' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ALB' as chave,
                '' as cobertura_temporal,
                'Albania' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ARE' as chave,
                '' as cobertura_temporal,
                'United Arab Emirates' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'AZE' as chave,
                '' as cobertura_temporal,
                'Azerbaijan' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BHR' as chave,
                '' as cobertura_temporal,
                'Bahrain' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CN09' as chave,
                '' as cobertura_temporal,
                'Shanghai' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CYP' as chave,
                '' as cobertura_temporal,
                'Cyprus' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'KAZ' as chave,
                '' as cobertura_temporal,
                'Kazakhstan' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'MAR' as chave,
                '' as cobertura_temporal,
                'Morocco' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'MKD' as chave,
                '' as cobertura_temporal,
                'North Macedonia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'MLT' as chave,
                '' as cobertura_temporal,
                'Malta' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'MNE' as chave,
                '' as cobertura_temporal,
                'Montenegro' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SGP' as chave,
                '' as cobertura_temporal,
                'Singapore' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SRB' as chave,
                '' as cobertura_temporal,
                'Serbia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'UZB' as chave,
                '' as cobertura_temporal,
                'Uzbekistan' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'VNM' as chave,
                '' as cobertura_temporal,
                'Viet Nam' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'XKV' as chave,
                '' as cobertura_temporal,
                'Kosovo' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'frequency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q12_1' as chave,
                '' as cobertura_temporal,
                'Full time: more than 90 percent of full time hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q12_2' as chave,
                '' as cobertura_temporal,
                'Part time: 50 70 percent of full time hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q12_3' as chave,
                '' as cobertura_temporal,
                'Part time: 71 90 percent of full time hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q12_4' as chave,
                '' as cobertura_temporal,
                'Part time: less than 50 percent of full time hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q14_1' as chave,
                '' as cobertura_temporal,
                'Weekly tasks related hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q15_1' as chave,
                '' as cobertura_temporal,
                'Weekly teaching hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q19_A_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q19_A_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q19_B_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q19_B_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q1_1' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q1_2' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q22_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q22_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q22_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q22_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_1' as chave,
                '' as cobertura_temporal,
                'Never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_2' as chave,
                '' as cobertura_temporal,
                'Once a year or less' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_3' as chave,
                '' as cobertura_temporal,
                '2-4 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_4' as chave,
                '' as cobertura_temporal,
                '5-10 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_5' as chave,
                '' as cobertura_temporal,
                '1-3 times a month' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_A_6' as chave,
                '' as cobertura_temporal,
                'Once a week or more' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_1' as chave,
                '' as cobertura_temporal,
                'Never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_2' as chave,
                '' as cobertura_temporal,
                'Once a year or less' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_3' as chave,
                '' as cobertura_temporal,
                '2-4 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_4' as chave,
                '' as cobertura_temporal,
                '5-10 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_5' as chave,
                '' as cobertura_temporal,
                '1-3 times a month' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_B_6' as chave,
                '' as cobertura_temporal,
                'Once a week or more' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_1' as chave,
                '' as cobertura_temporal,
                'Never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_2' as chave,
                '' as cobertura_temporal,
                'Once a year or less' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_3' as chave,
                '' as cobertura_temporal,
                '2-4 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_4' as chave,
                '' as cobertura_temporal,
                '5-10 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_5' as chave,
                '' as cobertura_temporal,
                '1-3 times a month' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_C_6' as chave,
                '' as cobertura_temporal,
                'Once a week or more' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_1' as chave,
                '' as cobertura_temporal,
                'Never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_2' as chave,
                '' as cobertura_temporal,
                'Once a year or less' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_3' as chave,
                '' as cobertura_temporal,
                '2-4 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_4' as chave,
                '' as cobertura_temporal,
                '5-10 times a year' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_5' as chave,
                '' as cobertura_temporal,
                '1-3 times a month' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q26_D_6' as chave,
                '' as cobertura_temporal,
                'Once a week or more' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q2_1' as chave,
                '' as cobertura_temporal,
                'Age' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q36_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q36_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_1' as chave,
                '' as cobertura_temporal,
                'Never or almost never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_2' as chave,
                '' as cobertura_temporal,
                'Occasionally' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_3' as chave,
                '' as cobertura_temporal,
                'Frequently' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_4' as chave,
                '' as cobertura_temporal,
                'Always' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_1' as chave,
                '' as cobertura_temporal,
                'Never or almost never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_2' as chave,
                '' as cobertura_temporal,
                'Occasionally' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_3' as chave,
                '' as cobertura_temporal,
                'Frequently' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_4' as chave,
                '' as cobertura_temporal,
                'Always' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_1' as chave,
                '' as cobertura_temporal,
                'Never or almost never' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_2' as chave,
                '' as cobertura_temporal,
                'Occasionally' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_3' as chave,
                '' as cobertura_temporal,
                'Frequently' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_4' as chave,
                '' as cobertura_temporal,
                'Always' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_A_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_A_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_A_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_A_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_B_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_B_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_B_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_B_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_C_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_C_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_C_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_C_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_D_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_D_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_D_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q77_D_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_A_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_A_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_A_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_A_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_B_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_B_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_B_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_B_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_C_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_C_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_C_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_C_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_D_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_D_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_D_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q78_D_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_A_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_A_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_A_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_A_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_B_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_B_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_B_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_B_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_C_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_C_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_C_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_C_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_D_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_D_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_D_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'measure' as nome_coluna,
                'Q80_D_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'H' as chave,
                '' as cobertura_temporal,
                'Hours' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_RESP' as chave,
                '' as cobertura_temporal,
                'Percentage of respondents' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'unit_measure' as nome_coluna,
                'Y' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'statistical_operation' as nome_coluna,
                'COUNT' as chave,
                '' as cobertura_temporal,
                'Count' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MEAN' as chave,
                '' as cobertura_temporal,
                'Arithmetic mean' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'statistical_operation' as nome_coluna,
                'SE' as chave,
                '' as cobertura_temporal,
                'Standard error' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'age' as nome_coluna,
                'Y30T49' as chave,
                '' as cobertura_temporal,
                'From 30 to 49 years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_GE50' as chave,
                '' as cobertura_temporal,
                '50 years or over' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'age' as nome_coluna,
                'Y_LT30' as chave,
                '' as cobertura_temporal,
                'Less than 30 years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'CITY' as chave,
                '' as cobertura_temporal,
                'City' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'RUR' as chave,
                '' as cobertura_temporal,
                'Rural area' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'TSUB' as chave,
                '' as cobertura_temporal,
                'Town and semi-dense area' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'institution_type' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'institution_type' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_GAP_10_30' as chave,
                '' as cobertura_temporal,
                '10 to 30 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_GT30' as chave,
                '' as cobertura_temporal,
                'Over 30 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_LT10' as chave,
                '' as cobertura_temporal,
                'Under 10 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'L_GAP_0_10' as chave,
                '' as cobertura_temporal,
                '0 to 10 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'L_GT10' as chave,
                '' as cobertura_temporal,
                'Over 10 percenr' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'NONE' as chave,
                '' as cobertura_temporal,
                'None' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_GAP_10_30' as chave,
                '' as cobertura_temporal,
                '10 to 30 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_GT30' as chave,
                '' as cobertura_temporal,
                'Over 30 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_LT10' as chave,
                '' as cobertura_temporal,
                'Under 10 percent' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'students_characteristics' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y6T10' as chave,
                '' as cobertura_temporal,
                '6 to 10 years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_GT10' as chave,
                '' as cobertura_temporal,
                'Over 10 years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_GT5_P_TALIS' as chave,
                '' as cobertura_temporal,
                'Over 5 years pre-TALIS' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_LT5' as chave,
                '' as cobertura_temporal,
                'Under 5 years' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_LT5_P_TALIS' as chave,
                '' as cobertura_temporal,
                'Under 5 years pre-TALIS' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'teacher_profile' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'talis_teacher' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ALB' as chave,
                '' as cobertura_temporal,
                'Albania' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ARE' as chave,
                '' as cobertura_temporal,
                'United Arab Emirates' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'AUS' as chave,
                '' as cobertura_temporal,
                'Australia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'AUT' as chave,
                '' as cobertura_temporal,
                'Austria' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'AZE' as chave,
                '' as cobertura_temporal,
                'Azerbaijan' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFL' as chave,
                '' as cobertura_temporal,
                'Flemish Community' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BEFR' as chave,
                '' as cobertura_temporal,
                'French Community' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BEL' as chave,
                '' as cobertura_temporal,
                'Belgium' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BGR' as chave,
                '' as cobertura_temporal,
                'Bulgaria' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BHR' as chave,
                '' as cobertura_temporal,
                'Bahrain' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'BRA' as chave,
                '' as cobertura_temporal,
                'Brazil' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CA48' as chave,
                '' as cobertura_temporal,
                'Alberta' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CHL' as chave,
                '' as cobertura_temporal,
                'Chile' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CN09' as chave,
                '' as cobertura_temporal,
                'Shanghai' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'COL' as chave,
                '' as cobertura_temporal,
                'Colombia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CRI' as chave,
                '' as cobertura_temporal,
                'Costa Rica' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CYP' as chave,
                '' as cobertura_temporal,
                'Cyprus' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'CZE' as chave,
                '' as cobertura_temporal,
                'Czechia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'DNK' as chave,
                '' as cobertura_temporal,
                'Denmark' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ESP' as chave,
                '' as cobertura_temporal,
                'Spain' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'EST' as chave,
                '' as cobertura_temporal,
                'Estonia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'FIN' as chave,
                '' as cobertura_temporal,
                'Finland' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'FRA' as chave,
                '' as cobertura_temporal,
                'France' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'HRV' as chave,
                '' as cobertura_temporal,
                'Croatia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'HUN' as chave,
                '' as cobertura_temporal,
                'Hungary' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ISL' as chave,
                '' as cobertura_temporal,
                'Iceland' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ISR' as chave,
                '' as cobertura_temporal,
                'Israel' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ITA' as chave,
                '' as cobertura_temporal,
                'Italy' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'JPN' as chave,
                '' as cobertura_temporal,
                'Japan' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'KAZ' as chave,
                '' as cobertura_temporal,
                'Kazakhstan' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'KOR' as chave,
                '' as cobertura_temporal,
                'Korea' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'LTU' as chave,
                '' as cobertura_temporal,
                'Lithuania' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'LVA' as chave,
                '' as cobertura_temporal,
                'Latvia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'MAR' as chave,
                '' as cobertura_temporal,
                'Morocco' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'MKD' as chave,
                '' as cobertura_temporal,
                'North Macedonia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'MLT' as chave,
                '' as cobertura_temporal,
                'Malta' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'MNE' as chave,
                '' as cobertura_temporal,
                'Montenegro' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'NLD' as chave,
                '' as cobertura_temporal,
                'Netherlands' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'NOR' as chave,
                '' as cobertura_temporal,
                'Norway' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'NZL' as chave,
                '' as cobertura_temporal,
                'New Zealand' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'OECD' as chave,
                '' as cobertura_temporal,
                'OECD' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'POL' as chave,
                '' as cobertura_temporal,
                'Poland' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'PRT' as chave,
                '' as cobertura_temporal,
                'Portugal' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ROU' as chave,
                '' as cobertura_temporal,
                'Romania' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SAU' as chave,
                '' as cobertura_temporal,
                'Saudi Arabia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SGP' as chave,
                '' as cobertura_temporal,
                'Singapore' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SRB' as chave,
                '' as cobertura_temporal,
                'Serbia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SVK' as chave,
                '' as cobertura_temporal,
                'Slovak Republic' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SVN' as chave,
                '' as cobertura_temporal,
                'Slovenia' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'SWE' as chave,
                '' as cobertura_temporal,
                'Sweden' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'TUR' as chave,
                '' as cobertura_temporal,
                'Türkiye' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'USA' as chave,
                '' as cobertura_temporal,
                'United States' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'UZB' as chave,
                '' as cobertura_temporal,
                'Uzbekistan' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'VNM' as chave,
                '' as cobertura_temporal,
                'Viet Nam' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'XKV' as chave,
                '' as cobertura_temporal,
                'Kosovo' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'reference_area' as nome_coluna,
                'ZAF' as chave,
                '' as cobertura_temporal,
                'South Africa' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'frequency' as nome_coluna,
                '_Z' as chave,
                '' as cobertura_temporal,
                'Not applicable' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q1_1' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q1_2' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_A_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_A_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_A_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_A_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_B_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_B_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_B_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_B_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_C_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_C_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_C_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_C_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_D_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_D_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_D_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q29_D_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q2_1' as chave,
                '' as cobertura_temporal,
                'Age' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q30_1' as chave,
                '' as cobertura_temporal,
                'Yes, for teachers with under one year of paid experience' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q30_2' as chave,
                '' as cobertura_temporal,
                'Yes, all teachers who are new to this school have access' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q30_3' as chave,
                '' as cobertura_temporal,
                'Yes, all teachers at this school have access' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q30_4' as chave,
                '' as cobertura_temporal,
                'No access to mentoring programmes for teachers in this school' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_A_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_A_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_A_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_A_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_B_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_B_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_B_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_B_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_C_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_C_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_C_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_C_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_D_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_D_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_D_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q39_D_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_A_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_A_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_A_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_A_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_B_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_B_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_B_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_B_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_C_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_C_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_C_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q40_C_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_A_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_A_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_B_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_B_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_C_1' as chave,
                '' as cobertura_temporal,
                'Yes' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q43_C_2' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q4_1' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_A_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_B_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_C_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_D_1' as chave,
                '' as cobertura_temporal,
                'Not at all' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_D_2' as chave,
                '' as cobertura_temporal,
                'To some extent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_D_3' as chave,
                '' as cobertura_temporal,
                'Quite a bit' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q51_D_4' as chave,
                '' as cobertura_temporal,
                'A lot' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_A_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_A_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_A_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_A_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_B_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_B_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_B_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_B_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_C_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_C_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_C_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_C_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_D_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_D_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_D_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q52_D_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_A_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_A_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_A_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_A_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_B_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_B_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_B_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_B_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_C_1' as chave,
                '' as cobertura_temporal,
                'Strongly disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_C_2' as chave,
                '' as cobertura_temporal,
                'Disagree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_C_3' as chave,
                '' as cobertura_temporal,
                'Agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q53_C_4' as chave,
                '' as cobertura_temporal,
                'Strongly agree' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q5_1' as chave,
                '' as cobertura_temporal,
                'Yes, more than 50 percent of my working hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q5_2' as chave,
                '' as cobertura_temporal,
                'Yes, up to 50 percent of my working hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q5_3' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q6_1' as chave,
                '' as cobertura_temporal,
                'Full time: more than 90 percent of full time hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q6_2' as chave,
                '' as cobertura_temporal,
                'Part time: 71 90 percent of full time hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q6_3' as chave,
                '' as cobertura_temporal,
                'Part time: 50 70 percent of full time hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q6_4' as chave,
                '' as cobertura_temporal,
                'Part time: less than 50 percent of full time hours' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_A_1' as chave,
                '' as cobertura_temporal,
                'Yes, in person' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_A_2' as chave,
                '' as cobertura_temporal,
                'Yes, virtual or online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_A_3' as chave,
                '' as cobertura_temporal,
                'Yes, in person and virtual/ online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_A_4' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_B_1' as chave,
                '' as cobertura_temporal,
                'Yes, in person' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_B_2' as chave,
                '' as cobertura_temporal,
                'Yes, virtual or online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_B_3' as chave,
                '' as cobertura_temporal,
                'Yes, in person and virtual/ online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_B_4' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_C_1' as chave,
                '' as cobertura_temporal,
                'Yes, in person' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_C_2' as chave,
                '' as cobertura_temporal,
                'Yes, virtual or online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_C_3' as chave,
                '' as cobertura_temporal,
                'Yes, in person and virtual/ online' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'measure' as nome_coluna,
                'Q8_C_4' as chave,
                '' as cobertura_temporal,
                'No' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'unit_measure' as nome_coluna,
                'PT_RESP' as chave,
                '' as cobertura_temporal,
                'Percentage of respondents' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'unit_measure' as nome_coluna,
                'Y' as chave,
                '' as cobertura_temporal,
                'Years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'statistical_operation' as nome_coluna,
                'COUNT' as chave,
                '' as cobertura_temporal,
                'Count' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'statistical_operation' as nome_coluna,
                'MEAN' as chave,
                '' as cobertura_temporal,
                'Arithmetic mean' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'statistical_operation' as nome_coluna,
                'SE' as chave,
                '' as cobertura_temporal,
                'Standard error' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_1' as chave,
                '' as cobertura_temporal,
                'Primary education' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_2' as chave,
                '' as cobertura_temporal,
                'Lower secondary education' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'education_level' as nome_coluna,
                'ISCED11_3' as chave,
                '' as cobertura_temporal,
                'Upper secondary education' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'age' as nome_coluna,
                'Y40T49' as chave,
                '' as cobertura_temporal,
                'From 40 to 49 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'age' as nome_coluna,
                'Y50T59' as chave,
                '' as cobertura_temporal,
                'From 50 to 59 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'age' as nome_coluna,
                'Y_GE60' as chave,
                '' as cobertura_temporal,
                '60 years or over' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'age' as nome_coluna,
                'Y_LT40' as chave,
                '' as cobertura_temporal,
                'Less than 40 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'age' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'sex' as nome_coluna,
                'F' as chave,
                '' as cobertura_temporal,
                'Female' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'sex' as nome_coluna,
                'M' as chave,
                '' as cobertura_temporal,
                'Male' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'sex' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'CITY' as chave,
                '' as cobertura_temporal,
                'City' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'RUR' as chave,
                '' as cobertura_temporal,
                'Rural area' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                'TSUB' as chave,
                '' as cobertura_temporal,
                'Town and semi-dense area' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'urbanisation_degree' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'institution_type' as nome_coluna,
                'INST_EDU_PRIV' as chave,
                '' as cobertura_temporal,
                'Private educational institutions' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'institution_type' as nome_coluna,
                'INST_EDU_PUB' as chave,
                '' as cobertura_temporal,
                'Public educational institutions' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'institution_type' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_GAP_10_30' as chave,
                '' as cobertura_temporal,
                '10 to 30 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_GT30' as chave,
                '' as cobertura_temporal,
                'Over 30 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'D_LT10' as chave,
                '' as cobertura_temporal,
                'Under 10 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'L_GAP_0_10' as chave,
                '' as cobertura_temporal,
                '0 to 10 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'L_GT10' as chave,
                '' as cobertura_temporal,
                'Over 10 percenr' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'NONE' as chave,
                '' as cobertura_temporal,
                'None' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_GAP_10_30' as chave,
                '' as cobertura_temporal,
                '10 to 30 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_GT30' as chave,
                '' as cobertura_temporal,
                'Over 30 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                'S_LT10' as chave,
                '' as cobertura_temporal,
                'Under 10 percent' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'students_characteristics' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y6T10' as chave,
                '' as cobertura_temporal,
                '6 to 10 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_GT10' as chave,
                '' as cobertura_temporal,
                'Over 10 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'teacher_profile' as nome_coluna,
                'Y_LT5' as chave,
                '' as cobertura_temporal,
                'Under 5 years' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'teacher_profile' as nome_coluna,
                '_T' as chave,
                '' as cobertura_temporal,
                'Total' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'decimals' as nome_coluna,
                '2' as chave,
                '' as cobertura_temporal,
                'Two' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'obs_status' as nome_coluna,
                'A' as chave,
                '' as cobertura_temporal,
                'Normal value' as valor
            ),
            struct(
                'talis_principal' as id_tabela,
                'unit_multiplier' as nome_coluna,
                '0' as chave,
                '' as cobertura_temporal,
                'Units' as valor
            )
        ]
    )
