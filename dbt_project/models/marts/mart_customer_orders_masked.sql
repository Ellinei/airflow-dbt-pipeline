{{
  config(
    materialized = 'view',
    schema       = 'marts',
    post_hook    = "{{ grant_select('analyst') }}"
  )
}}

-- Analyst-facing view: email is masked to protect PII.
-- engineer_user connects to mart_customer_orders (full data).
-- analyst_user connects here and never sees the raw email column.
SELECT
    customer_id,
    regexp_replace(email, '(^.{1})[^@]+(@.+$)', '\1****\2') AS email,
    lifetime_value,
    total_orders,
    completed_orders,
    cancelled_orders
FROM {{ ref('mart_customer_orders') }}
