-- stg_customers: clean and type-cast the raw customers seed
-- Materialised as a VIEW (cheap, always current with the source)

with source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        customer_id::int                        as customer_id,
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,
        lower(trim(email))                      as email,
        upper(trim(country))                    as country_code,
        created_at::date                        as created_at

    from source

)

select * from renamed
