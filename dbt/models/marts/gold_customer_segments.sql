{{ config(materialized='incremental', unique_key='customer_id', incremental_strategy='merge') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
    {% if is_incremental() %}
    -- Recompute only customers with activity since the last run; the orders CTE
    -- above is unfiltered, so each recomputed customer's lifetime totals stay complete.
    where customer_id in (
        select customer_id from orders
        where created_at >= (select coalesce(max(last_order_at), '1900-01-01') from {{ this }})
    )
    {% endif %}
)
select
    c.customer_id,
    c.segment,
    c.region,
    count(o.order_id)                          as lifetime_orders,
    coalesce(sum(o.total_amount), 0)           as lifetime_value,
    coalesce(round(avg(o.total_amount), 2), 0) as avg_order_value,
    max(o.created_at)                          as last_order_at
from customers c
left join orders o on c.customer_id = o.customer_id
group by c.customer_id, c.segment, c.region
