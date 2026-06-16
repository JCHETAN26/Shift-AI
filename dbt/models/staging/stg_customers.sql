with source as (
    select * from {{ source('raw', 'customers') }}
)
select
    customer_id,
    name,
    lower(email)   as email,
    lower(segment) as segment,
    region,
    created_at,
    updated_at
from source
