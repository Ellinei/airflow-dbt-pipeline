-- mart_olist_customer_orders: one row per unique Olist customer with
-- aggregated order metrics. Grouped by customer_unique_id — NOT customer_id,
-- since Olist mints a new customer_id per order (grouping by customer_id
-- would make every "customer" trivially show exactly 1 order).

with orders as (

    select * from {{ ref('stg_olist_orders') }}

),

customers as (

    select * from {{ ref('stg_olist_customers') }}

),

order_amounts as (

    select
        order_id,
        sum(payment_value) as amount

    from {{ ref('stg_olist_order_payments') }}
    group by order_id

),

orders_with_amount as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        coalesce(a.amount, 0.00) as amount

    from orders o
    left join order_amounts a using (order_id)

),

customer_orders as (

    select
        c.customer_unique_id,
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.amount

    from orders_with_amount o
    inner join customers c using (customer_id)

),

order_summary as (

    select
        customer_unique_id,
        count(order_id)                                            as total_orders,
        sum(amount)                                                as lifetime_value,
        avg(amount)                                                as avg_order_value,
        min(order_purchase_timestamp)                              as first_order_date,
        max(order_purchase_timestamp)                              as last_order_date,
        count(order_id) filter (where order_status = 'delivered')  as delivered_orders,
        count(order_id) filter (where order_status = 'canceled')   as cancelled_orders

    from customer_orders
    group by customer_unique_id

)

select * from order_summary
