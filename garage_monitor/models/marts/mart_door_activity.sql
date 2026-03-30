with events as (
    select * from {{ ref('int_door_events') }}
),

final as (
    select
        event_id,
        event_time,
        burst_signal_count,
        data_hex,
        code_string,

        -- Date parts
        event_time::date                                        as event_date,
        extract(hour from event_time)::integer                  as hour_of_day,
        to_char(event_time, 'Day')                              as day_of_week_name,
        extract(isodow from event_time)::integer                as day_of_week_num,   -- 1=Mon, 7=Sun
        extract(week from event_time)::integer                  as week_of_year,
        to_char(event_time, 'Mon YYYY')                         as month_label,

        -- Convenience flags
        case
            when extract(isodow from event_time) in (6, 7) then true
            else false
        end                                                     as is_weekend,

        case
            when extract(hour from event_time) between 5  and 11 then 'Morning'
            when extract(hour from event_time) between 12 and 16 then 'Afternoon'
            when extract(hour from event_time) between 17 and 20 then 'Evening'
            else 'Night'
        end                                                     as time_of_day_bucket

    from events
)

select * from final
