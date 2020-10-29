with orders as (
    
    select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from {{ source('jaffle_shop','orders') }}

),

mapped_values as (

    select * from {{ ref('status_mapping') }}

),

joined as (

    select  
        orders.*,
        mapped_values.mapped_value,
        case
            when order.status in ('')
            else 'pending'
        end as mapped_value
    from orders
    left join mapped_values using (status)

)

select * from joined