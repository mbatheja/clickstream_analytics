with segment_totals as (
    select device, variant, acquisition_channel, sum(total_exists) as total_segment_exits
      from {{ref('fct_session_dropoff')}}
     group by device, variant, acquisition_channel
)
select d.device, d.variant, d.acquisition_channel, d.exit_event,
       d.exit_percentage, s.total_segment_exits
  from {{ ref('fct_session_dropoff') }} d
        join segment_totals s
        on d.device = s.device
        and d.variant = s.variant
        and d.acquisition_channel = s.acquisition_channel
 where d.exit_percentage >= 100.0
   and s.total_segment_exits >= 10
