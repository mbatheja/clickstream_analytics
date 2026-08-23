with ranked_events as (
    select
        session_id, 
        user_id,
        device,
        variant,
        acquisition_channel, 
        event_type,
        event_timestamp,
        row_number() over (partition by session_id order by event_timestamp desc) as event_rank,
        count(*) over (partition by session_id) as total_session_events
    from {{ref('stg_events')}}
)

select device, variant, acquisition_channel, 
       event_type as exit_event, count(distinct session_id) as total_exits,
       count(case when total_session_events = 1 then 1 end) as bounced_sessions,
       round(100.0 * count(distinct session_id)/ sum(count(distinct session_id)) over (partition by device, variant), 2) as exit_percentage
  from ranked_events
 where event_rank = 1
 group by device, variant, acquisition_channel, exit_event
