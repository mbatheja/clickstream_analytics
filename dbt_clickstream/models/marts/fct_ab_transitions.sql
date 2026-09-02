select
    variant,
    device,
    acquisition_channel,
    user_type,
    case when grouping(device) = 1 then 'variant_level' else 'segment_level' end as grain,

    count(distinct session_id) as total_sessions,

    round(avg(homepage_to_search_seconds), 2) as avg_homepage_to_search_seconds,
    round(avg(search_to_product_view_seconds), 2) as avg_search_to_product_view_seconds,
    round(avg(product_view_to_add_to_cart_seconds), 2) as avg_product_view_to_add_to_cart_seconds,
    round(avg(add_to_cart_to_checkout_seconds), 2) as avg_add_to_cart_to_checkout_seconds,
    round(avg(checkout_to_purchase_seconds), 2) as avg_checkout_to_purchase_seconds

from {{ ref('int_step_transition') }}
group by grouping sets (
    (variant, device, acquisition_channel, user_type),
    (variant)
)
