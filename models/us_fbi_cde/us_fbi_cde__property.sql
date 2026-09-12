{{
    config(
        alias="property",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(property_description_id as string) property_description_id,
    safe_cast(property_id as string) property_id,
    safe_cast(incident_id as string) incident_id,
    safe_cast(property_loss_code as string) property_loss_code,
    safe_cast(property_description_code as string) property_description_code,
    safe_cast(property_value as int64) property_value,
    safe_cast(date_recovered as date) date_recovered,
    safe_cast(stolen_count as int64) stolen_count,
    safe_cast(recovered_count as int64) recovered_count,
    safe_cast(suspected_drug_code as string) suspected_drug_code,
    safe_cast(drug_quantity as float64) drug_quantity,
    safe_cast(drug_measure_code as string) drug_measure_code
from {{ set_datalake_project("us_fbi_cde_staging.property") }} as t
