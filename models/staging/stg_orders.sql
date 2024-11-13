{{ config ( materialized = 'incremental'  , unique_key='orderid') }}
 
 
select * from

{{source('my_raw_config', 'orders')}}
{% if is_incremental() %}

where orderdate>( select max(orderdate) from {{this }})

{% endif %}