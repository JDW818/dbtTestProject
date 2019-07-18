with source1 as (

    select * from {{ ref('customers_upload') }}

),

source2 as (
    
    select * from {{ ref('orders_upload') }}
),

calc as (

    select 
        source1.id as customer_id, 
        lag(source2.created_at, 1) [ignore nulls] 
        over (partition by source1.id
        order by source2.created_at desc) as created_at_prev,
        created_at
    from source1
    
    inner join source2
            
            on source1.id = source2.customer_id

)

    select 
        datediff(month, created_at, created_at_prev) as months_since_prior_order
    from calc