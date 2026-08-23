with session_data as (
    select * from {{ ref('int_sessions') }}
),

variant_summary as (
    select
        variant,
        
        count(distinct session_id) as total_sessions,
        count(distinct user_id) as total_users,
        count(case when homepage_views = 1 then 1 end) as sessions_homepage,
        count(case when has_viewed_product = 1 then 1 end) as sessions_product_viewed,
        count(case when added_to_cart = 1 then 1 end) as sessions_added_to_cart,
        count(case when has_checkout = 1 then 1 end) as sessions_checkout,
        count(case when has_purchased = 1 then 1 end) as total_conversions,

        round(
            count(case when has_purchased = 1 then 1 end) * 1.0 / nullif(count(distinct session_id), 0), 4
        ) as overall_conversion_rate,

        round(count(case when has_purchased = 1 then 1 end) * 1.0 / nullif(count(case when has_checkout = 1 then 1 end), 0), 4
        ) as checkout_to_purchase_rate,

        round(avg(session_duration), 2) as avg_session_duration_seconds

    from session_data
    group by 1
)

select * from variant_summary