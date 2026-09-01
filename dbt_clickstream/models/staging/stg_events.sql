with source_data as (
    select * from {{source('raw_clickstream', 'clickstream_events')}}
)

select
    raw_payload:event_id::string as event_id,
    raw_payload:user_id::string as user_id,
    raw_payload:session_id::string as session_id,
    raw_payload:event_type::string as event_type,
    raw_payload:timestamp::timestamp_ntz as event_timestamp,
    raw_payload:customer_metadata:experiment_variant::string as variant,
    raw_payload:customer_metadata:acquisition_channel::string as acquisition_channel,
    raw_payload:customer_metadata:signup_date::date as signup_date,
    raw_payload:device::string as device,
    ingested_at
from source_data
