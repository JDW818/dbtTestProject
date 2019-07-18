with source as (

    select * from {{ ref('order_items_upload') }}

),

renamed as (

    select
        id as order_items_upload_id,
        order_id,
        price,
        quantity,
        size,
        color,
        product_id
    from source

)

select * from renamed