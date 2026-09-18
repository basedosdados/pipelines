{{
    config(
        schema="us_cms_hcris",
        alias="dicionario",
        materialized="table",
    )
}}

-- CMS publishes codes without labels, so unlike a dataset whose facts
-- carry both there is nothing here to derive the meanings from. Every
-- value below is transcribed from HCRIS_DataDictionary.csv or from CMS
-- Publication 15-2 section 4004.1; see code/codes.py for which.
select id_tabela, nome_coluna, chave, cobertura_temporal, valor
from
    (
        select
            'report' as id_tabela,
            'form_version' as nome_coluna,
            '2552-96' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-96, filed for cost reporting periods before May 2010'
            as valor
        union all
        select
            'report' as id_tabela,
            'form_version' as nome_coluna,
            '2552-10' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-10, filed for cost reporting periods beginning on or after 1 May 2010'
            as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '1' as chave,
            '' as cobertura_temporal,
            'Voluntary nonprofit, church' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '2' as chave,
            '' as cobertura_temporal,
            'Voluntary nonprofit, other' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '3' as chave,
            '' as cobertura_temporal,
            'Proprietary, individual' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '4' as chave,
            '' as cobertura_temporal,
            'Proprietary, corporation' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '5' as chave,
            '' as cobertura_temporal,
            'Proprietary, partnership' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '6' as chave,
            '' as cobertura_temporal,
            'Proprietary, other' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '7' as chave,
            '' as cobertura_temporal,
            'Governmental, federal' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '8' as chave,
            '' as cobertura_temporal,
            'Governmental, city-county' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '9' as chave,
            '' as cobertura_temporal,
            'Governmental, county' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '10' as chave,
            '' as cobertura_temporal,
            'Governmental, state' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '11' as chave,
            '' as cobertura_temporal,
            'Governmental, hospital district' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '12' as chave,
            '' as cobertura_temporal,
            'Governmental, city' as valor
        union all
        select
            'report' as id_tabela,
            'provider_control_type_code' as nome_coluna,
            '13' as chave,
            '' as cobertura_temporal,
            'Governmental, other' as valor
        union all
        select
            'report' as id_tabela,
            'report_status_code' as nome_coluna,
            '1' as chave,
            '' as cobertura_temporal,
            'As submitted' as valor
        union all
        select
            'report' as id_tabela,
            'report_status_code' as nome_coluna,
            '2' as chave,
            '' as cobertura_temporal,
            'Settled without audit' as valor
        union all
        select
            'report' as id_tabela,
            'report_status_code' as nome_coluna,
            '3' as chave,
            '' as cobertura_temporal,
            'Settled with audit' as valor
        union all
        select
            'report' as id_tabela,
            'report_status_code' as nome_coluna,
            '4' as chave,
            '' as cobertura_temporal,
            'Reopened' as valor
        union all
        select
            'report' as id_tabela,
            'report_status_code' as nome_coluna,
            '5' as chave,
            '' as cobertura_temporal,
            'Amended' as valor
        union all
        select
            'report' as id_tabela,
            'initial_report_indicator' as nome_coluna,
            'Y' as chave,
            '' as cobertura_temporal,
            'Yes, the first cost report filed for this provider' as valor
        union all
        select
            'report' as id_tabela,
            'initial_report_indicator' as nome_coluna,
            'N' as chave,
            '' as cobertura_temporal,
            'No' as valor
        union all
        select
            'report' as id_tabela,
            'last_report_indicator' as nome_coluna,
            'Y' as chave,
            '' as cobertura_temporal,
            'Yes, the final cost report filed for this provider' as valor
        union all
        select
            'report' as id_tabela,
            'last_report_indicator' as nome_coluna,
            'N' as chave,
            '' as cobertura_temporal,
            'No' as valor
        union all
        select
            'report' as id_tabela,
            'last_report_indicator' as nome_coluna,
            'X' as chave,
            '' as cobertura_temporal,
            'Value present in the source but not documented by CMS' as valor
        union all
        select
            'report' as id_tabela,
            'adr_vendor_code' as nome_coluna,
            '2' as chave,
            '' as cobertura_temporal,
            'Ernst & Young' as valor
        union all
        select
            'report' as id_tabela,
            'adr_vendor_code' as nome_coluna,
            '3' as chave,
            '' as cobertura_temporal,
            'KPMG' as valor
        union all
        select
            'report' as id_tabela,
            'adr_vendor_code' as nome_coluna,
            '4' as chave,
            '' as cobertura_temporal,
            'HFS' as valor
        union all
        select
            'report' as id_tabela,
            'utilization_code' as nome_coluna,
            'F' as chave,
            '' as cobertura_temporal,
            'Full Medicare utilization' as valor
        union all
        select
            'report' as id_tabela,
            'utilization_code' as nome_coluna,
            'L' as chave,
            '' as cobertura_temporal,
            'Low Medicare utilization' as valor
        union all
        select
            'report' as id_tabela,
            'utilization_code' as nome_coluna,
            'N' as chave,
            '' as cobertura_temporal,
            'No Medicare utilization' as valor
        union all
        select
            'report_value' as id_tabela,
            'form_version' as nome_coluna,
            '2552-96' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-96, filed for cost reporting periods before May 2010'
            as valor
        union all
        select
            'report_value' as id_tabela,
            'form_version' as nome_coluna,
            '2552-10' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-10, filed for cost reporting periods beginning on or after 1 May 2010'
            as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'form_version' as nome_coluna,
            '2552-96' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-96, filed for cost reporting periods before May 2010'
            as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'form_version' as nome_coluna,
            '2552-10' as chave,
            '' as cobertura_temporal,
            'CMS Form 2552-10, filed for cost reporting periods beginning on or after 1 May 2010'
            as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'report_status_code' as nome_coluna,
            '1' as chave,
            '' as cobertura_temporal,
            'As submitted' as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'report_status_code' as nome_coluna,
            '2' as chave,
            '' as cobertura_temporal,
            'Settled without audit' as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'report_status_code' as nome_coluna,
            '3' as chave,
            '' as cobertura_temporal,
            'Settled with audit' as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'report_status_code' as nome_coluna,
            '4' as chave,
            '' as cobertura_temporal,
            'Reopened' as valor
        union all
        select
            'hospital_financial' as id_tabela,
            'report_status_code' as nome_coluna,
            '5' as chave,
            '' as cobertura_temporal,
            'Amended' as valor
    )
