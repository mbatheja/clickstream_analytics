select 
    date_trunc('day', session_start) as session_date,
    device,
    variant,
    count(distinct session_id) as total_sessions, 
    count(distinct user_id) as unique_users,
    round(avg(session_duration), 2) as avg_session_duration,
    count(case when has_checkout = true then 1 end) as sessions_with_checkout,
    count(case when has_purchased = true then 1 end) as sessions_with_purchase,
    
    -- Calculated Rates (with divide-by-zero protection and proper rounding)
    round(
        (count(case when has_checkout = true then 1 end) * 100.0) / nullif(count(distinct session_id), 0), 2) as checkout_rate,
    
    round((count(case when has_purchased = true then 1 end) * 100.0) / nullif(count(distinct session_id), 0), 2) as conversion_rate,
    
    100 - round((count(case when has_purchased = true then 1 end) * 100.0) / nullif(count(case when has_checkout = true then 1 end), 0), 2) as drop_off_rate

from {{ ref('int_sessions') }}
group by session_date, device