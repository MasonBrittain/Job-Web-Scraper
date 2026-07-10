-- Typed, deduplicated view over the raw parquet landing zone.
-- One row per (source, company, job_id, ingest_date) snapshot.

with source as (

    select * from {{ source('landing', 'job_postings') }}

)

select
    source,
    company,
    job_id,
    title,
    department,
    location,
    url,
    try_cast(published_at as timestamptz)  as published_at,
    description_text,
    try_cast(ingested_at as timestamptz)   as ingested_at,
    cast(ingest_date as date)              as ingest_date
from source
-- a partition is overwritten on re-run, but guard against duplicates anyway
qualify row_number() over (
    partition by source, company, job_id, ingest_date
    order by ingested_at desc
) = 1
