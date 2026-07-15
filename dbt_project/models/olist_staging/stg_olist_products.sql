-- stg_olist_products: clean and type-cast the raw Olist products table
-- Materialised as a VIEW. Renames the upstream "lenght" column-name typos.

with source as (

    select * from {{ source('olist_raw', 'olist_products') }}

),

renamed as (

    select
        product_id::text                            as product_id,
        trim(product_category_name)                 as product_category_name,
        product_name_lenght::int                    as product_name_length,
        product_description_lenght::int             as product_description_length,
        product_photos_qty::int                      as product_photos_qty,
        product_weight_g::numeric                    as product_weight_g,
        product_length_cm::numeric                   as product_length_cm,
        product_height_cm::numeric                   as product_height_cm,
        product_width_cm::numeric                    as product_width_cm

    from source

)

select * from renamed
