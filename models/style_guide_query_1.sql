{{config(materialized = 'table', sort = 'order_id', dist = 'order_id')}}

    select 
        orders.field_1,
    orders.field_2,
        orders.field_3,
        case when orders.cancellation_date is null and orders.expiration_date is not null then expiration_date when orders.cancellation_date is null then orders.start_date + 7 else orders.cancellation_date
        end as cancellation_date,
        items_agg.total_field_4,
        items_agg.max_field_5
    from {{ ref('stg_orders') }} orders
    left join (
        select sum(field_4) as total_field_4, order_id, max(field_5) as max_field_5from {{ ref('stg_order_items') }}
    group by order_id) as items_agg on orders.order_id = items_agg.order_id
    where orders.field_1 = 'abc' and (orders.field_2 = 'def' or orders.field_2 = 'ghi')
    having count(*) > 1