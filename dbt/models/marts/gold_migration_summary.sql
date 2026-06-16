{{ config(materialized='table') }}

-- Unique to Shift.ai: a per-table migration scorecard on the Gold side.
-- target_rows come from what landed in Snowflake RAW; source_rows and
-- reconciliation_status are enriched by the reconciliation job (Phase 7) via
-- RAW.MIGRATION_METADATA. Left-joined so this still builds before the first
-- reconciliation run (status shows PENDING).

{% set tables = ['customers', 'products', 'orders', 'inventory', 'shipments'] %}

with target_counts as (
    {% for t in tables %}
    select '{{ t }}' as table_name, count(*) as target_rows from {{ source('raw', t) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
recon as (
    {% if adapter.get_relation(database=source('raw','migration_metadata').database,
                               schema=source('raw','migration_metadata').schema,
                               identifier='migration_metadata') %}
    select * from {{ source('raw', 'migration_metadata') }}
    {% else %}
    select
        null::varchar  as table_name,   null::number    as source_rows,
        null::boolean  as count_match,  null::boolean   as checksum_match,
        null::number   as sample_mismatches,
        null::varchar  as reconciliation_status, null::varchar as schema_version,
        null::timestamp as reconciled_at
    where false
    {% endif %}
)
select
    tc.table_name,
    r.source_rows,
    tc.target_rows,
    coalesce(r.source_rows, tc.target_rows) - tc.target_rows as row_delta,
    coalesce(r.count_match, false)                           as count_match,
    r.checksum_match,
    coalesce(r.sample_mismatches, 0)                         as sample_mismatches,
    coalesce(r.reconciliation_status, 'PENDING')             as reconciliation_status,
    coalesce(r.schema_version, 'v1')                         as schema_version,
    coalesce(r.reconciled_at, current_timestamp())          as last_synced_at
from target_counts tc
left join recon r on tc.table_name = r.table_name
order by tc.table_name
