with source as (

    select * from {{ ref('customers_upload') }}

),

renamed as (

    select
        id as customer_id,
        email,
        created_at,
        first_name,
        last_name,
        gender,
        accepts_marketing,
        {{ dbt_utils.surrogate_key('id') }} as unique_id
    from source

)

select * from renamed