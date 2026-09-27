{{
    config(
        schema="us_nih_reporter",
        alias="publication",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(pmid as string) pmid,
    safe_cast(pmc_id as string) pmc_id,
    safe_cast(issn as string) issn,
    safe_cast(publication_title as string) publication_title,
    safe_cast(journal_title as string) journal_title,
    safe_cast(journal_title_abbreviation as string) journal_title_abbreviation,
    safe_cast(journal_volume as string) journal_volume,
    safe_cast(journal_issue as string) journal_issue,
    safe_cast(page_number as string) page_number,
    safe_cast(publication_date as string) publication_date,
    safe_cast(publication_year as int64) publication_year,
    safe_cast(author_list as string) author_list,
    safe_cast(affiliation as string) affiliation,
    safe_cast(country as string) country,
    safe_cast(language as string) language
from {{ set_datalake_project("us_nih_reporter_staging.publication") }} as t
