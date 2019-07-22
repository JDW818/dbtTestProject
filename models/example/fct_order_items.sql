{# 
create a fct_order_items table joining orders to order items. 
select all fields from order items and join in the order_date, 
email and customer_id from the orders table
#}
with orderitems as (

    select * from {{ ref('order_items_upload') }}

),

orders as (

    select * from {{ ref('orders_upload') }}

),

final as (

    select
        orderitems.id as order_item_id,
        orderitems.order_id,
        orderitems.price,
        orderitems.quantity,
        orderitems.size,
        orderitems.color,
        orderitems.product_id,
        orders.created_at,
        orders.email,
        orders.customer_id

    from orderitems
    
    inner join orders on orderitems.order_id = orders.id

)

select * from final