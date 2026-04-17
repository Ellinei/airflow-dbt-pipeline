-- mart_customer_orders: one row per customer with aggregated order metrics
-- Materialised as a TABLE — fast to query from BI tools (e.g. Power BI)

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

order_summary as (

    select
        customer_id,
        count(order_id)                                     as total_orders,
        sum(amount)                                         as lifetime_value,
        avg(amount)                                         as avg_order_value,
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,
        count(order_id) filter (where status = 'completed') as completed_orders,
        count(order_id) filter (where status = 'cancelled') as cancelled_orders

    from orders
    group by customer_id

)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country_code,
    c.created_at                                            as customer_since,
    coalesce(o.total_orders, 0)                             as total_orders,
    coalesce(o.lifetime_value, 0.00)                        as lifetime_value,
    coalesce(o.avg_order_value, 0.00)                       as avg_order_value,
    o.first_order_date,
    o.last_order_date,
    coalesce(o.completed_orders, 0)                         as completed_orders,
    coalesce(o.cancelled_orders, 0)                         as cancelled_orders

from customers c
left join order_summary o using (customer_id)
