{# 
create a `fct_order_items` table joining orders to customers to get the 
`order_date` and `email` and `customer_id` on the order table
#}
with source_1 as (

    select * from {{ ref('orders_upload') }}

),

source_2 as (

    select * from {{ ref('customers_upload') }}

)

final as (

    select
        source_1.created_at,
        source_2.email,
        source_2.customer_id,

    from source_1
    
    left join source_2 on source_1.customer_id  = source_2.id

)

select * from renamed