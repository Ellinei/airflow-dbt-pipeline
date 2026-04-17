-- stg_orders: clean and type-cast the raw orders seed
-- Materialised as a VIEW

with source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        order_id::int                           as order_id,
        customer_id::int                        as customer_id,
        order_date::date                        as order_date,
        lower(trim(status))                     as status,
        amount::numeric(10, 2)                  as amount

    from source

)

select * from renamed
