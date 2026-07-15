-- stg_olist_sellers: clean and type-cast the raw Olist sellers table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_sellers') }}

),

renamed as (

    select
        seller_id::text                    as seller_id,
        seller_zip_code_prefix::text       as seller_zip_code_prefix,
        lower(trim(seller_city))          as seller_city,
        upper(trim(seller_state))         as seller_state

    from source

)

select * from renamed
