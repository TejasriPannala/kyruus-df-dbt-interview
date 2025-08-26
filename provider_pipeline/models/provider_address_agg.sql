with provider_with_addresses as (

    select 
        p.id as provider_id,
        p.first_name || ' ' || p.last_name as provider_name,
        coalesce(
            array_agg(
                struct_pack(
                    address_id := a.id,
                    street := a.street,
                    rank := a.rank
                )
            ) filter (where a.id is not null), 
            []
        ) as addresses
    from {{ ref('providers') }} p
    left join {{ ref('addresses') }} a 
        on p.id = a.id
    group by p.id, p.first_name, p.last_name

)

select * from provider_with_addresses
