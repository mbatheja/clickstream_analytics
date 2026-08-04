with events as (
    select *
      from {{ ref('stg_events')}}
),
    session_metrics as (
select session_id,
       user_id,
       max(device_type) as device,
       max(variant) as variant,
       min(event_timestamp) as session_start, 
       max(event_timestamp) as session_end, 
       datediff('second', min(event_timestamp), max(event_timestamp)) as session_duration, 
       count(event_id) as total_events, 
       count(case when event_type = 'page_view' then 1 end) as page_views,
       max(case when event_type = 'checkout' then 1 else 0 end)::boolean as has_checkout,
       max(case when event_type = 'purchase' then 1 else 0 end)::boolean as has_purchased
  from {{ ref('stg_events') }}
  group by session_id, user_id
  )

select * from session_metrics
