select cohort_date, variant, cohort_size, newest_users, first_week_users, d1_retention_rate, d7_retention_rate, d30_retention_rate
  from {{ ref('fct_user_retention')}}
 where newest_users > cohort_size,
       or first_week_users > cohort_size
       or second_week_users > cohort_size
       or d1_retention_rate > 100.0
       or d1_retention_rate < 0.0
       or d7_retention_rate > 100.0
       or d30_retention_rate > 100.0