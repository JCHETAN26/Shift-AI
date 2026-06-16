{{ config(materialized='table') }}

select
    i.product_id,
    p.name      as product_name,
    p.category,
    sum(i.quantity)                                              as total_on_hand,
    min(i.reorder_point)                                         as reorder_point,
    count(distinct i.warehouse_id)                               as warehouse_count,
    case
        when sum(i.quantity) = 0 then 'OUT_OF_STOCK'
        when max(case when i.needs_reorder then 1 else 0 end) = 1 then 'REORDER'
        else 'HEALTHY'
    end                                                          as health_status
from {{ ref('stg_inventory') }} i
join {{ ref('stg_products') }} p on i.product_id = p.product_id
group by i.product_id, p.name, p.category
