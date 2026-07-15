-- stg_olist_customers: clean and type-cast the raw Olist customers table
-- Materialised as a VIEW. Note: customer_id is order-scoped (Olist mints a
-- new one per order); customer_unique_id is the real repeat-customer key.

with source as (

    select * from {{ source('olist_raw', 'olist_customers') }}

),

renamed as (

    select
        customer_id::text                as customer_id,
        customer_unique_id::text         as customer_unique_id,
        customer_zip_code_prefix::text   as customer_zip_code_prefix,
        lower(trim(customer_city))       as customer_city,
        upper(trim(customer_state))      as customer_state

    from source

)

select * from renamed
