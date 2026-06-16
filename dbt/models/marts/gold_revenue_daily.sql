{{ config(materialized='incremental', unique_key='revenue_date', incremental_strategy='merge') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date >= (select coalesce(max(revenue_date), '1900-01-01') from {{ this }})
    {% endif %}
)
select
    order_date                                                  as revenue_date,
    count(*)                                                    as order_count,
    count(distinct customer_id)                                 as unique_customers,
    sum(total_amount)                                           as gross_revenue,
    sum(case when status = 'cancelled' then total_amount else 0 end) as cancelled_revenue,
    sum(case when status <> 'cancelled' then total_amount else 0 end) as net_revenue,
    round(avg(total_amount), 2)                                 as avg_order_value
from orders
group by order_date
