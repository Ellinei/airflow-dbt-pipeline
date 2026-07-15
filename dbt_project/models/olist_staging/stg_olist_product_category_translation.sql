-- stg_olist_product_category_translation: clean and type-cast the raw
-- category-translation table. Materialised as a VIEW.

with source as (

    select * from {{ source('olist_raw', 'olist_product_category_translation') }}

),

renamed as (

    select
        trim(product_category_name)             as product_category_name,
        trim(product_category_name_english)      as product_category_name_english

    from source

)

select * from renamed
