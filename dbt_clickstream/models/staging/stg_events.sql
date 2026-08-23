with source_data as (
    select * from {{source('raw_clickstream', 'raw_events')}}
)

select
    payload:event_id::string as event_id,
    payload:user_id::string as user_id,
    payload:session_id::string as session_id,
    payload:event_type::string as event_type,
    payload:timestamp::timestamp_ntz as event_timestamp,
    payload:page::string as page,
    payload:customer_metadata:experiment_variant::string as variant,
    payload:customer_metadata:acquisition_channel::string as acquistion_channel,
    payload:customer_metadata:signup_date::date as signup_date,
    payload:device::string as device,
    ingested_at
from source_data
