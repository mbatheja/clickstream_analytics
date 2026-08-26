with user_session as (
    select user_id, variant, acquisition_channel,
            max(user_signup_date) as signup_date,
            date_trunc('day', session_start) as session_date,
            timestampdiff('day', max(user_signup_date), date_trunc('day', session_start)) as days_since_signup
    from {{ref('int_sessions')}}
    group by user_id, variant, acquisition_channel, session_date
),

cohort_base as (
    select date_trunc('day', signup_date) as cohort_date,
           variant, acquisition_channel,
           count(distinct user_id) as cohort_size
      from user_session
     group by cohort_date, variant, acquisition_channel
)

select c.cohort_date, c.variant, c.acquisition_channel, c.cohort_size,
       count(distinct case when s.days_since_signup = 0 then s.user_id end) as newest_users,
       count(distinct case when s.days_since_signup between 6 and 8 then s.user_id end) as first_week_users,
       count(distinct case when s.days_since_signup between 13 and 15 then s.user_id end) as second_week_users,
       count(distinct case when s.days_since_signup between 28 and 30 then s.user_id end) as month_users,

       round(100.0 * count(distinct case when s.days_since_signup = 1 then s.user_id end) / nullif(c.cohort_size, 0), 2) as d1_retention_rate,
       round(100.0 * count(distinct case when s.days_since_signup between 6 and 8 then s.user_id end) / nullif(c.cohort_size, 0), 2) as d7_retention_rate,
       round(100.0 * count(distinct case when s.days_since_signup between 13 and 15 then s.user_id end) / nullif(c.cohort_size, 0), 2) as d14_retention_rate

 from cohort_base as c left join user_session as s on  c.cohort_date = date_trunc('day', s.signup_date)
      and c.variant = s.variant
      and c.acquisition_channel = s.acquisition_channel
      group by c.cohort_date, c.variant, c.acquisition_channel, c.cohort_size
