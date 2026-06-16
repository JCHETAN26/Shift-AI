with source as (
    select * from {{ source('raw', 'products') }}
)
select
    product_id,
    name,
    lower(category) as category,
    unit_cost,
    is_active,
    created_at
from source
