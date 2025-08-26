with split_degrees as (

    select
        p.id as provider_id,
        p.first_name || ' ' || p.last_name as provider_name,
        unnest(string_split(p.degrees, ',')) as degree
    from {{ ref('providers') }} p

),

ranked_degrees as (

    select
        s.provider_id,
        s.provider_name,
        d.ptui,
        d.rank,
        row_number() over (
            partition by s.provider_id
            order by d.rank asc
        ) as rn
    from split_degrees s
    join {{ ref('degree_types') }} d
        on trim(s.degree) = d.degree
)

select
    provider_id,
    provider_name,
    ptui
from ranked_degrees
where rn = 1