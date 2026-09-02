select
    device,
    variant,
    acquisition_channel,
    user_type,
    count(distinct session_id) as total_sessions,
    count(distinct user_id) as unique_users,
    round(avg(session_duration), 2) as avg_session_duration,
    count(case when homepage_views > 0 then 1 end) as sessions_with_homepage_view,
    count(case when has_searched_product > 0 then 1 end) as sessions_with_product_search,
    count(case when has_viewed_product > 0 then 1 end) as sessions_with_product_view,
    count(case when added_to_cart > 0 then 1 end) as sessions_with_cart_adds,
    count(case when has_checkout then 1 end) as sessions_with_checkout,
    count(case when has_purchased then 1 end) as sessions_with_purchase,

    -- Calculated Rates (with divide-by-zero protection and proper rounding)
    round((count(case when has_viewed_product > 0 then 1 end) * 100.0) / nullif(count(case when homepage_views > 0 then 1 end), 0), 2) as homepage_to_product_rate,
    round((count(case when added_to_cart > 0 then 1 end) * 100.0) / nullif(count(case when has_viewed_product > 0 then 1 end), 0), 2) as product_to_cart_rate,
    round((count(case when has_checkout then 1 end) * 100.0) / nullif(count(case when added_to_cart > 0 then 1 end), 0), 2) as cart_to_checkout_rate,
    round((count(case when has_purchased then 1 end) * 100.0) / nullif(count(case when has_checkout then 1 end), 0), 2) as checkout_to_purchase_rate,

    round(100.0 * (1.0 - (count(case when has_checkout then 1 end) * 1.0 / nullif(count(case when added_to_cart > 0 then 1 end),0))), 2) as cart_abandonment_rate,
    round(100.0 * (1.0 - (count(case when has_purchased then 1 end) * 1.0 / nullif(count(case when has_checkout then 1 end), 0))), 2) as checkout_abandonment_rate,
    round(100.0 * count(case when has_purchased then 1 end) / nullif(count(distinct session_id), 0), 2) as overall_conversion_rate

from {{ ref('int_sessions') }}
group by device, variant, acquisition_channel, user_type