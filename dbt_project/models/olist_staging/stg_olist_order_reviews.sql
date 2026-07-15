-- stg_olist_order_reviews: clean and type-cast the raw Olist reviews table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_reviews') }}

),

renamed as (

    select
        review_id::text                          as review_id,
        order_id::text                            as order_id,
        review_score::int                         as review_score,
        trim(review_comment_title)                as review_comment_title,
        trim(review_comment_message)              as review_comment_message,
        review_creation_date::timestamp           as review_creation_date,
        review_answer_timestamp::timestamp        as review_answer_timestamp

    from source

)

select * from renamed
