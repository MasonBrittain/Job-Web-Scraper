-- One row per (posting, skill) pair: postings joined to the skills seed by
-- regex match over the description text. The seed owns the taxonomy, so
-- adding a skill is a one-line CSV change followed by `dbt build`.

with postings as (

    select
        posting_key,
        source,
        company,
        title,
        department,
        first_seen_date,
        is_active,
        lower(description_text) as description
    from {{ ref('fct_job_postings') }}

),

skills as (

    select * from {{ ref('skills') }}

)

select
    postings.posting_key,
    postings.source,
    postings.company,
    postings.title,
    postings.department,
    postings.first_seen_date,
    postings.is_active,
    skills.skill,
    skills.category
from postings
inner join skills
    on regexp_matches(postings.description, skills.pattern)
