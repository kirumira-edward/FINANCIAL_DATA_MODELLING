-- Use a wider date range to ensure all transaction dates are covered
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )
    }}
),

dates as (
    select
        cast(date_day as date) as date_day
    from date_spine
),

enriched_dates as (
    select
        date_day,
        date_part('year', date_day)::integer as year,
        date_part('month', date_day)::integer as month_number,
        to_char(date_day, 'Month') as month_name,
        date_part('quarter', date_day)::integer as quarter,
        date_part('day', date_day)::integer as day_of_month,
        date_part('dow', date_day)::integer as day_of_week,
        to_char(date_day, 'Day') as day_name,
        date_part('doy', date_day)::integer as day_of_year,
        date_part('week', date_day)::integer as week_of_year,
        (date_part('dow', date_day) >= 5)::boolean as is_weekend,
        false as is_holiday, 
        date_day = date_trunc('month', date_day)::date as is_first_day_of_month,
        date_day = (date_trunc('month', date_day) + interval '1 month' - interval '1 day')::date as is_last_day_of_month
    from dates
)

select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_number,
    month_name,
    quarter,
    year,
    is_weekend,
    is_holiday,
    is_first_day_of_month,
    is_last_day_of_month
from enriched_dates
order by date_day