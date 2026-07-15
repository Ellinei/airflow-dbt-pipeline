-- mart_olist_seller_performance: one row per seller with revenue, freight,
-- and review-score aggregates across all their order items.

with sellers as (

    select * from {{ ref('stg_olist_sellers') }}

),

order_items as (

    select * from {{ ref('stg_olist_order_items') }}

),

reviews as (

    select * from {{ ref('stg_olist_order_reviews') }}

),

item_reviews as (

    select
        oi.seller_id,
        oi.order_id,
        oi.price,
        oi.freight_value,
        r.review_score

    from order_items oi
    left join reviews r using (order_id)

),

seller_summary as (

    select
        seller_id,
        count(distinct order_id) as total_orders,
        sum(price)               as total_revenue,
        sum(freight_value)       as total_freight,
        avg(review_score)        as avg_review_score,
        count(review_score)      as reviewed_orders

    from item_reviews
    group by seller_id

)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    coalesce(ss.total_orders, 0)      as total_orders,
    coalesce(ss.total_revenue, 0.00)  as total_revenue,
    coalesce(ss.total_freight, 0.00)  as total_freight,
    ss.avg_review_score,
    coalesce(ss.reviewed_orders, 0)   as reviewed_orders

from sellers s
left join seller_summary ss using (seller_id)
