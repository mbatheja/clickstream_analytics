with segment_sums as (
    select device, variant, round(sum(exit_percentage), 1) as total_segment_pct
      from {{ref('fct_session_dropoff')}}
     group by device, variant
)

select *
  from segment_sums
 where total_segment_pct not between 99.0 and 101.0