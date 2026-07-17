-- Business invariant: completed + cancelled orders can never exceed total
-- orders. Generic schema tests can't express a cross-column check like this
-- one, hence a singular test. Returns offending rows — the test fails if
-- this query returns any.
select
    customer_id,
    total_orders,
    completed_orders,
    cancelled_orders
from {{ ref('mart_customer_orders') }}
where completed_orders + cancelled_orders > total_orders
