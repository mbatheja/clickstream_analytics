with events as (
    select *
      from {{ ref('stg_events')}}
),
    session_metrics as (
select session_id,
       user_id,
       max(device) as device,
       max(variant) as variant,
       max(acquisition_channel) as acquisition_channel,
       max(signup_date) as user_signup_date,
       case
            when max(signup_date) = date_trunc('day', min(event_timestamp)) then 'new_user'
            else 'returning_user'
       end as user_type,
       min(event_timestamp) as session_start,
       max(event_timestamp) as session_end,
       datediff('second', min(event_timestamp), max(event_timestamp)) as session_duration,
       count(event_id) as total_events,
       count(case when event_type = 'view_homepage' then 1 end) as homepage_views,
       count(case when event_type = 'search_product' then 1 end) as has_searched_product,
       count(case when event_type = 'view_product_details' then 1 end) as has_viewed_product,
       count(case when event_type = 'add_to_cart' then 1 end) as added_to_cart,
       max(case when event_type = 'checkout_initiated' then 1 else 0 end)::boolean as has_checkout,
       max(case when event_type = 'purchase_completed' then 1 else 0 end)::boolean as has_purchased
  from events
  group by session_id, user_id, variant, device
  )

select * from session_metrics
