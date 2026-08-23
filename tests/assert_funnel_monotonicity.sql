select session_id
  from {{ ref('int_sessions') }}
 where (has_purchased = 1 and has_checkout = 0)
        or (has_checkout = 1 and added_to_cart = 0)
        or (added_to_cart = 1 and has_viewed_product = 0)
        or (has_viewed_product = 1 and has_searched_product = 0)
        or (has_searched_product = 0 and homepage_views = 0)