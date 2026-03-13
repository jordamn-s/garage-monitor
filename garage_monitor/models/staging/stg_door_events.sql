with source as (

    select * from {{ source('garage_monitor', 'raw_events') }}

),

staged as (

    select
        id,
        received_at,
        device_time,
        model                           as device_model,
        count                           as signal_count,
        num_rows,
        rows -> 0 ->> 'data'            as data_hex,
        (rows -> 0 ->> 'len')::integer  as bit_length,
        codes ->> 0                     as code_string,
        raw_payload

    from source

)

select * from staged
