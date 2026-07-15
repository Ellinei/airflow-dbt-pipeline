-- stg_olist_order_payments: clean and type-cast the raw Olist payments table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_payments') }}

),

renamed as (

    select
        order_id::text                    as order_id,
        payment_sequential::int           as payment_sequential,
        lower(trim(payment_type))         as payment_type,
        payment_installments::int         as payment_installments,
        payment_value::numeric(10, 2)     as payment_value

    from source

)

select * from renamed
