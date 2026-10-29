{{
    config(
        alias="indicator",
        schema="se_gu_vdem",
        materialized="table",
    )
}}
select
    safe_cast(variable_id as string) variable_id,
    safe_cast(codebook_tag as string) codebook_tag,
    safe_cast(category as string) category,
    safe_cast(variants as string) variants,
    safe_cast(name as string) name,
    safe_cast(variable_type as string) variable_type,
    safe_cast(question as string) question,
    safe_cast(clarification as string) clarification,
    safe_cast(responses as string) responses,
    safe_cast(scale as string) scale,
    safe_cast(notes as string) notes,
    safe_cast(cross_coder_aggregation as string) cross_coder_aggregation,
    safe_cast(country_year_aggregation as string) country_year_aggregation,
    safe_cast(data_release as string) data_release,
    safe_cast(years as string) years,
    safe_cast(convergence as string) convergence,
    safe_cast(project_manager as string) project_manager
from {{ set_datalake_project("se_gu_vdem_staging.indicator") }} as t
