with source as (
    select * from {{ source('raw', 'orders') }}
)
select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    lower(status)  as status,
    lower(channel) as channel,
    created_at,
    updated_at,
    cast(created_at as date) as order_date
from source
