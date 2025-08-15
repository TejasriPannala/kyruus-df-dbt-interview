with addresses as (
  select * from {{ ref('addresses')}}
)

select count(*) as address_count from addresses
