{# 
create a fct_order_items table joining orders to order items. 
select all fields from order items and join in the order_date, 
email and customer_id from the orders table
#}
with source_1 as (

    select * from {{ ref('order_items_upload') }}

),

source_2 as (

    select * from {{ ref('orders_upload') }}

)

final as (

    select
        source1.id as order_item_id,
        source1.order_id,
        source1.price,
        source1.quantity,
        source1.size,
        source1.color,
        source1.product_id,
        source2.created_at,
        source2.email,
        source2.customer_id

    from source_1
    
    inner join source_2 on source_1.order_id = source2.id

)

select * from renamed