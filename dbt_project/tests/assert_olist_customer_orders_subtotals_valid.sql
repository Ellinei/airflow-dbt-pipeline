-- Business invariant: delivered + cancelled orders can never exceed total
-- orders. Returns offending rows — the test fails if this query returns any.
select
    customer_unique_id,
    total_orders,
    delivered_orders,
    cancelled_orders
from {{ ref('mart_olist_customer_orders') }}
where delivered_orders + cancelled_orders > total_orders
