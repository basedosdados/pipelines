{{
    config(
        alias="rollcalls",
        schema="us_ucla_voteview",
        materialized="table",
        partition_by={
            "field": "congress",
            "data_type": "int64",
            "range": {"start": 1, "end": 130, "interval": 1},
        },
    )
}}
select
    safe_cast(congress as int64) congress,
    safe_cast(chamber as string) chamber,
    safe_cast(rollnumber as string) rollnumber,
    safe_cast(date as date) date,
    safe_cast(session as string) session,
    safe_cast(clerk_rollnumber as string) clerk_rollnumber,
    safe_cast(majority_requirement as string) majority_requirement,
    safe_cast(yea_count as int64) yea_count,
    safe_cast(nay_count as int64) nay_count,
    safe_cast(nominate_mid_1 as float64) nominate_mid_1,
    safe_cast(nominate_mid_2 as float64) nominate_mid_2,
    safe_cast(nominate_spread_1 as float64) nominate_spread_1,
    safe_cast(nominate_spread_2 as float64) nominate_spread_2,
    safe_cast(nominate_log_likelihood as float64) nominate_log_likelihood,
    safe_cast(bill_number as string) bill_number,
    safe_cast(vote_result as string) vote_result,
    safe_cast(vote_desc as string) vote_desc,
    safe_cast(vote_question as string) vote_question,
    safe_cast(dtl_desc as string) dtl_desc
from {{ set_datalake_project("us_ucla_voteview_staging.rollcalls") }} as t
