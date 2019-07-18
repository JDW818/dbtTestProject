with source as (

    select * from {{ ref('orders_upload') }}

),

renamed as (

    select
        id as order_id,
        email,
        customer_id,
        created_at,
        total,
        completed,
        ip_address,
        street_address,
        billing_country_code,
        referral_domain,
        referral_url
    from source

)

select * from renamed