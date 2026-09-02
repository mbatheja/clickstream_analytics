select *
  from {{ ref('int_step_transition') }}
 where homepage_to_search_seconds < 0
    or search_to_product_view_seconds < 0
    or product_view_to_add_to_cart_seconds < 0
    or add_to_cart_to_checkout_seconds < 0
    or checkout_to_purchase_seconds < 0