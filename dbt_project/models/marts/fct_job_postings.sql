-- One row per unique posting, with its observation lifecycle derived from
-- daily snapshots: when it first appeared, when it was last seen, and whether
-- it is still live (present in the most recent snapshot).

with snapshots as (

    select * from {{ ref('stg_job_postings') }}

),

bounds as (

    select
        source,
        company,
        job_id,
        min(ingest_date) as first_seen_date,
        max(ingest_date) as last_seen_date
    from snapshots
    group by 1, 2, 3

),

latest as (

    select *
    from snapshots
    qualify row_number() over (
        partition by source, company, job_id
        order by ingest_date desc
    ) = 1

),

latest_ingest as (

    select max(ingest_date) as max_ingest_date from snapshots

)

select
    latest.source || ':' || latest.company || ':' || latest.job_id as posting_key,
    latest.source,
    latest.company,
    latest.job_id,
    latest.title,
    latest.department,
    latest.location,
    latest.url,
    latest.published_at,
    bounds.first_seen_date,
    bounds.last_seen_date,
    bounds.last_seen_date = latest_ingest.max_ingest_date          as is_active,
    date_diff('day', bounds.first_seen_date, bounds.last_seen_date) as days_observed,
    latest.description_text
from latest
inner join bounds using (source, company, job_id)
cross join latest_ingest
