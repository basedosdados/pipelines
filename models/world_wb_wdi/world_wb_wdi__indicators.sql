{{ config(schema="world_wb_wdi", alias="indicators", materialized="table") }}


select
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(topic as string) topic,
    safe_cast(short_definition as string) short_definition,
    safe_cast(long_definition as string) long_definition,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(periodicity as string) periodicity,
    safe_cast(base_period as string) base_period,
    safe_cast(other_notes as string) other_notes,
    safe_cast(aggregation_method as string) aggregation_method,
    safe_cast(limitations_exceptions as string) limitations_exceptions,
    safe_cast(notes_from_original_source as string) notes_from_original_source,
    safe_cast(general_comments as string) general_comments,
    safe_cast(source as string) source,
    safe_cast(
        statistical_concept_methodology as string
    ) statistical_concept_methodology,
    safe_cast(development_relevance as string) development_relevance,
    safe_cast(related_source_links as string) related_source_links,
    safe_cast(other_web_links as string) other_web_links,
    safe_cast(related_indicators as string) related_indicators,
    safe_cast(license_type as string) license_type,
from {{ set_datalake_project("world_wb_wdi_staging.indicators") }} as t
