{{
  config(
    materialized = 'table',
    sort = 'order_id',
    dist = 'order_id'
  )
}}

with

orders as (

    select * from {{ ref('stg_orders') }}

),

items as (

    select * from {{ ref('stg_order_items') }}

),

items_agg as (

    select
        order_id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5

    from items
    group by 1

),

final as (

    select 
        orders.field_1,
        orders.field_2,
        orders.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when orders.cancellation_date is null
                and orders.expiration_date is not null
                then expiration_date
            when orders.cancellation_date is null
                then orders.start_date + 7
            else orders.cancellation_date
        end as cancellation_date,

        items_agg.total_field_4,
        items_agg.max_field_5

    from orders
    left join items_agg  
        on orders.order_id = items_agg.order_id
    where orders.field_1 = 'abc'
        and (
            orders.field_2 = 'def' or
            orders.field_2 = 'ghi'
        )
    having count(*) > 1

)

select * from final