{{ config(materialized = 'incremental', unique_key = ['orderid', 'lineno']) }}
 
select
 
od.*,
o.orderdate as orderdate
 
from
 
{{source('my_raw_config', 'orders')}} as o inner join
 
{{source('my_raw_config', 'order_details')}} as od
 
on o.orderid = od.orderid
 
{% if is_incremental() %}
 
  -- this filter will only be applied on an incremental run
  where orderdate > (select max(orderdate) from {{this}})
 
{% endif %}