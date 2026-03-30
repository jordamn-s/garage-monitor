with source as (
    select * from {{ ref('stg_door_events') }}
),

-- Calculate the gap in seconds between each signal and the one before it
with_gaps as (
    select
        *,
        extract(epoch from (
            device_time - lag(device_time) over (order by device_time)
        )) as seconds_since_prev
    from source
),

-- A new event starts when the gap exceeds 2 seconds (update after histogram analysis)
-- We flag each row that opens a new burst, then assign a group ID
event_boundaries as (
    select
        *,
        case
            when seconds_since_prev is null
              or seconds_since_prev > 2   -- <-- replace this number after histogram
            then 1
            else 0
        end as is_new_event
    from with_gaps
),

-- Running sum of the flags = unique event group number
event_groups as (
    select
        *,
        sum(is_new_event) over (order by device_time rows unbounded preceding) as event_group
    from event_boundaries
),

-- Collapse each group to one row: anchor to first signal, count the burst size
final as (
    select
        event_group                     as event_id,
        min(device_time)                as event_time,
        count(*)                        as burst_signal_count,
        max(data_hex)                   as data_hex,
        max(code_string)                as code_string
    from event_groups
    group by event_group
)

select * from final
