{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

with fct_orders as (
    select 1 as order_id, 101 as customer_id,
           [struct(1001 as product_id), struct(1002 as product_id)] as items
),
dim_customers as (
    select 101 as customer_id, 'bob' as customer_name
),
unnest_orders as (
    select *
    from fct_orders, unnest(items) with offset as offset_val
),
joined as (
    select unnest_orders.*, dim_customers.customer_name
    from unnest_orders left join dim_customers using (customer_id)
)
select offset_val from joined
