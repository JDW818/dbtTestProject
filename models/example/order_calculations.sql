with orders as (

    select * from {{ ref('stg_orders') }}

),

orderitems as (
    
    select * from {{ ref('stg_order_items') }}
),

calc as (

    select 
        orders.customer_id, 
        lag(orders.created_at, 1) [ignore nulls] 
        over (partition by orders.customer_id
              order by orders.created_at desc) as created_at_prev,
        created_at
    from orders
),

final as (
    select 
        datediff(month, created_at, created_at_prev) as months_since_prior_order
    from calc
)

select * from final