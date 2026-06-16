with source as (
    select * from {{ source('raw', 'inventory') }}
)
select
    inventory_id,
    product_id,
    warehouse_id,
    quantity,
    reorder_point,
    (quantity <= reorder_point) as needs_reorder,
    updated_at
from source
