with step_timestamps as (

    select
        session_id,
        min(case when event_type = 'view_homepage' then event_timestamp end) as homepage_ts,
        min(case when event_type = 'search_product' then event_timestamp end) as search_ts,
        min(case when event_type = 'view_product_details' then event_timestamp end) as product_view_ts,
        min(case when event_type = 'add_to_cart' then event_timestamp end) as add_to_cart_ts,
        min(case when event_type = 'checkout_initiated' then event_timestamp end) as checkout_initiated_ts,
        min(case when event_type = 'purchase_completed' then event_timestamp end) as purchase_completed_ts

    from {{ ref('stg_events') }}
    group by session_id

)

select
    s.session_id,
    i.user_id,
    i.device,
    i.variant,
    i.acquisition_channel,
    i.user_type,
    i.session_end,
    s.homepage_ts,
    s.search_ts,
    s.product_view_ts,
    s.add_to_cart_ts,
    s.checkout_initiated_ts,
    s.purchase_completed_ts,
    datediff('second', s.homepage_ts, s.search_ts) as homepage_to_search_seconds,
    datediff('second', s.search_ts, s.product_view_ts) as search_to_product_view_seconds,
    datediff('second', s.product_view_ts, s.add_to_cart_ts) as product_view_to_add_to_cart_seconds,
    datediff('second', s.add_to_cart_ts, s.checkout_initiated_ts) as add_to_cart_to_checkout_seconds,
    datediff('second', s.checkout_initiated_ts, s.purchase_completed_ts) as checkout_to_purchase_seconds

from step_timestamps s
join {{ ref('int_sessions') }} i on s.session_id = i.session_id
