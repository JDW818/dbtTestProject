select id as customer_id from {{ ref('customers_upload') }}