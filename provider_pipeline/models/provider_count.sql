with providers as (
  select * from {{ ref('providers')}}
)

select count(*) as provider_count from providers
