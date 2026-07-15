-- stg_olist_order_items: clean and type-cast the raw Olist order-items table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_items') }}

),

renamed as (

    select
        order_id::text                   as order_id,
        order_item_id::int               as order_item_id,
        product_id::text                 as product_id,
        seller_id::text                  as seller_id,
        shipping_limit_date::timestamp   as shipping_limit_date,
        price::numeric(10, 2)            as price,
        freight_value::numeric(10, 2)    as freight_value

    from source

)

select * from renamed
