{{
    config(
        schema="us_cfpb_complaints",
        alias="complaint",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2031, "interval": 1},
        },
        cluster_by=["product", "company_name", "state_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(complaint_id as string) complaint_id,
    safe_cast(state_id as string) state_id,
    safe_cast(date_received as date) date_received,
    safe_cast(date_sent_to_company as date) date_sent_to_company,
    safe_cast(product as string) product,
    safe_cast(sub_product as string) sub_product,
    safe_cast(issue as string) issue,
    safe_cast(sub_issue as string) sub_issue,
    safe_cast(company_name as string) company_name,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(tags as string) tags,
    safe_cast(submitted_via as string) submitted_via,
    safe_cast(company_public_response as string) company_public_response,
    safe_cast(company_response_to_consumer as string) company_response_to_consumer,
    safe_cast(timely_response as string) timely_response,
    safe_cast(consumer_complaint_narrative as string) consumer_complaint_narrative
from {{ set_datalake_project("us_cfpb_complaints_staging.complaint") }} as t
