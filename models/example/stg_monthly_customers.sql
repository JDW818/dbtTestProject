/*Create "all months" date spine. then use the spine to create a model called
 `monthly_customers` that shows one row per month per customer since their 
 first order date until now. Hint: dbt utils `date_spine` macro. 
 Submit a PR to Jeremy.
*/

with customers as 
(
    select * from {{ ref('stg_customers') }}
),

all_months as 
(
    {{ dbt_utils.date_spine(
      datepart="month",
      start_date="to_date('01/01/2015', 'mm/dd/yyyy')",
      end_date="dateadd(week, 1, current_date)"
      )
    }}
),

calc as
(
    select
      customer_id,
      email,
      created_at,
      rank () over(partition by customer_id, month(created_at), year(created_at) order by created_at DESC) rnk
    from customers
    inner join all_months on month(customers.created_at) = month(all_months.date_month)
    and year(customers.created_at) = year(all_months.date_month)
)

    select 
      customer_id,
      email,
      created_at
    from calc
    where rnk = 1