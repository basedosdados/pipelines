{{
    config(
        schema="us_cms_hcris",
        alias="hospital_financial",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2031, "interval": 1},
        },
        cluster_by=["provider_ccn"],
    )
}}

-- One row per cost report, with the named measures the published
-- worksheet-line-column mappings define. Built from the report_value model
-- rather than from staging so the mapping is applied to typed values, and so
-- the provenance of every measure below is the SQL itself.
--
-- The grain is the report, not the hospital-year. CMS states plainly that one
-- hospital can file two or more reports for the same year -- a fiscal year
-- change, or a change of ownership -- and the collapse rules the published
-- research mappings use for that disagree with each other. Publishing the
-- report grain leaves that choice to the user; fiscal_year_begin_date,
-- fiscal_year_end_date and fiscal_year_days are the columns to make it with.
with
    cells as (
        select
            report_id,
            form_version,
            worksheet_code,
            line_number,
            column_number,
            numeric_value,
            alpha_value
        from {{ ref("us_cms_hcris__report_value") }}
        -- Only the 14 worksheets the mapping reads, out of the 884 HCRIS
        -- publishes. With the model clustered on the cell address this prunes
        -- most of the table before the pivot.
        where
            worksheet_code in (
                'A700001',
                'A700002',
                'D10A181',
                'D30A180',
                'D40A180',
                'E00A18A',
                'G000000',
                'G200000',
                'G300000',
                'S100000',
                'S100001',
                'S200000',
                'S200001',
                'S300001'
            )
    ),
    pivoted as (
        select
            form_version,
            report_id,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00300'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00200'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as hospital_name,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as street_address,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00200'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00101'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as city,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00200'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00101'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as reported_state_abbreviation,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00200'
                                    and column_number = '00300'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00101'
                                    and column_number = '0300'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as zip_code,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '00200'
                                    and column_number = '00400'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '00101'
                                    and column_number = '0400'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as county_name,
            max(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S200001'
                                    and line_number = '14100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S200000'
                                    and line_number = '04001'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then alpha_value
                end
            ) as chain_organization_name,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_patient_charges,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00200'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00200'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then abs(numeric_value)
                end
            ) as contractual_allowances_discounts,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00300'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00300'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as net_patient_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00400'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00400'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_operating_expense,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00600'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00600'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as donations,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00700'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '00700'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as investment_income,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '02500'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '02500'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as other_income,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '02800'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G300000'
                                    and line_number = '03000'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_other_expense,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '00100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_hospital_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01000'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '00900'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_general_routine_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01600'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01500'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_intensive_care_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01700'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01600'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_routine_care_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01800'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01700'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_ancillary_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01900'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01800'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_outpatient_service_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02800'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02500'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_total_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01800'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01700'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as outpatient_ancillary_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01900'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '01800'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as outpatient_outpatient_service_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02800'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02500'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as outpatient_total_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02800'
                                    and column_number = '00300'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G200000'
                                    and line_number = '02500'
                                    and column_number = '0300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_patient_revenue,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02400'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as cost_to_charge_ratio,
            sum(
                case
                    when
                        (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '03000'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as uncompensated_care_charges,
            sum(
                case
                    when
                        (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '03100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as uncompensated_care_cost,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02000'
                                    and column_number = '00300'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02000'
                                    and column_number = '00300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_initial_charity_care_charges,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02200'
                                    and column_number = '00300'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02200'
                                    and column_number = '00300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as charity_care_partial_payments,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02100'
                                    and column_number = '00300'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02100'
                                    and column_number = '00300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as cost_of_initial_charity_care,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02300'
                                    and column_number = '00300'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02300'
                                    and column_number = '00300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as cost_of_charity_care,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02600'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02600'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_bad_debt_expense,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02700'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02700'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as medicare_reimbursable_bad_debt,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02800'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02800'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as non_medicare_bad_debt_expense,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '02900'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '02900'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as non_reimbursable_bad_debt_cost,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S100000'
                                    and line_number = '03000'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'S100001'
                                    and line_number = '03000'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as cost_of_uncompensated_care,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '00300'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as available_bed_days_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '00800'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '0600'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as inpatient_bed_days_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '01500'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '1500'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as discharges_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '01300'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '1300'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as discharges_medicare_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '01400'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00100'
                                    and column_number = '1400'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as discharges_medicaid_adult_pediatric,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00700'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '00500'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_total_adult_pediatric_swing,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '00800' and '00899'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '02600' and '02619'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_intensive_care_unit,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '00900' and '00999'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '02700' and '02719'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_coronary_care_unit,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '01000' and '01099'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '02800' and '02819'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_burn_intensive_care_unit,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '01100' and '01199'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '02900' and '02919'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_surgical_intensive_care_unit,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '01200' and '01299'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number between '02040' and '02059'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'S300001'
                                    and line_number between '02060' and '02079'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'S300001'
                                    and line_number between '02080' and '02099'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'S300001'
                                    and line_number between '02120' and '02139'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'S300001'
                                    and line_number between '02140' and '02159'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'S300001'
                                    and line_number between '02180' and '02199'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_other_special_care,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '01400'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '01200'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_total,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '02700'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'S300001'
                                    and line_number = '02500'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as beds_grand_total,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '00100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '00100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as cash,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '01100'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '01100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_current_assets,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '03000'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '02100'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_fixed_assets,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '04500'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '03600'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_current_liabilities,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '01400'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01600'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01800'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '02000'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '02200'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '02400'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '02600'
                                    and column_number = '00100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '02800'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'G000000'
                                    and line_number = '01301'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01401'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01501'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01601'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01701'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01801'
                                    and column_number = '0100'
                                )
                                or (
                                    worksheet_code = 'G000000'
                                    and line_number = '01901'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then abs(numeric_value)
                end
            ) as accumulated_depreciation,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'A700001'
                                    and line_number = '01000'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'A700002'
                                    and line_number = '00900'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as new_capital_assets,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '05900'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '01600'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as total_medicare_payment,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '06000'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '01700'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as secondary_medicare_payment,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '07093'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as value_based_purchasing_adjustment,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'E00A18A'
                                    and line_number = '07094'
                                    and column_number = '00100'
                                )
                            )
                        )
                    then abs(numeric_value)
                end
            ) as readmissions_reduction_adjustment,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'D10A181'
                                    and line_number = '04900'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'D10A181'
                                    and line_number = '04900'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as medicare_inpatient_total_cost,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'D10A181'
                                    and line_number = '05300'
                                    and column_number = '00100'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'D10A181'
                                    and line_number = '05300'
                                    and column_number = '0100'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as medicare_inpatient_operating_cost,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'D30A180'
                                    and line_number between '03000' and '03599'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'D40A180'
                                    and line_number between '02500' and '03099'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as medicare_inpatient_routine_charges,
            sum(
                case
                    when
                        (
                            form_version = '2552-10'
                            and (
                                (
                                    worksheet_code = 'D30A180'
                                    and line_number = '20200'
                                    and column_number = '00200'
                                )
                            )
                        )
                        or (
                            form_version = '2552-96'
                            and (
                                (
                                    worksheet_code = 'D40A180'
                                    and line_number = '10300'
                                    and column_number = '0200'
                                )
                            )
                        )
                    then numeric_value
                end
            ) as medicare_inpatient_ancillary_net_charges
        from cells
        -- Grouped by the real key. report_id alone would merge the 107 ids CMS
        -- reused across the two form versions into single rows holding two
        -- unrelated hospitals' values.
        group by form_version, report_id
    )

select
    r.year,
    r.report_id,
    r.provider_ccn,
    r.state_id,
    r.state_abbreviation,
    r.form_version,
    r.fiscal_year_begin_date,
    r.fiscal_year_end_date,
    r.fiscal_year_days,
    r.report_status_code,
    p.hospital_name,
    p.street_address,
    p.city,
    p.reported_state_abbreviation,
    p.zip_code,
    p.county_name,
    p.chain_organization_name,
    p.total_patient_charges,
    p.contractual_allowances_discounts,
    p.net_patient_revenue,
    p.total_operating_expense,
    p.donations,
    p.investment_income,
    p.other_income,
    p.total_other_expense,
    p.inpatient_hospital_revenue,
    p.inpatient_general_routine_revenue,
    p.inpatient_intensive_care_revenue,
    p.inpatient_routine_care_revenue,
    p.inpatient_ancillary_revenue,
    p.inpatient_outpatient_service_revenue,
    p.inpatient_total_revenue,
    p.outpatient_ancillary_revenue,
    p.outpatient_outpatient_service_revenue,
    p.outpatient_total_revenue,
    p.total_patient_revenue,
    p.cost_to_charge_ratio,
    p.uncompensated_care_charges,
    p.uncompensated_care_cost,
    p.total_initial_charity_care_charges,
    p.charity_care_partial_payments,
    p.cost_of_initial_charity_care,
    p.cost_of_charity_care,
    p.total_bad_debt_expense,
    p.medicare_reimbursable_bad_debt,
    p.non_medicare_bad_debt_expense,
    p.non_reimbursable_bad_debt_cost,
    p.cost_of_uncompensated_care,
    p.beds_adult_pediatric,
    p.available_bed_days_adult_pediatric,
    p.inpatient_bed_days_adult_pediatric,
    p.discharges_adult_pediatric,
    p.discharges_medicare_adult_pediatric,
    p.discharges_medicaid_adult_pediatric,
    p.beds_total_adult_pediatric_swing,
    p.beds_intensive_care_unit,
    p.beds_coronary_care_unit,
    p.beds_burn_intensive_care_unit,
    p.beds_surgical_intensive_care_unit,
    p.beds_other_special_care,
    p.beds_total,
    p.beds_grand_total,
    p.cash,
    p.total_current_assets,
    p.total_fixed_assets,
    p.total_current_liabilities,
    p.accumulated_depreciation,
    p.new_capital_assets,
    p.total_medicare_payment,
    p.secondary_medicare_payment,
    p.value_based_purchasing_adjustment,
    p.readmissions_reduction_adjustment,
    p.medicare_inpatient_total_cost,
    p.medicare_inpatient_operating_cost,
    p.medicare_inpatient_routine_charges,
    p.medicare_inpatient_ancillary_net_charges
from {{ ref("us_cms_hcris__report") }} as r
left join pivoted as p on p.report_id = r.report_id and p.form_version = r.form_version
